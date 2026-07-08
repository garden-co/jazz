import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocket } from "undici";
import { afterEach, describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { webSocketUrl } from "./native-runtime/websocket.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { hasJazzNapiBuild, loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { SubscriptionManager } from "./subscription-manager.js";
import type { WasmRow } from "../drivers/types.js";

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

const CHAT_POLICY_SCHEMA: WasmSchema = {
  chats: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "visibility", column_type: { type: "Text" }, nullable: false },
      { name: "owner_id", column_type: { type: "Text" }, nullable: false },
    ],
    policies: {
      select: {
        using: {
          type: "Or",
          exprs: [
            {
              type: "Cmp",
              column: "visibility",
              op: "Eq",
              value: { type: "Literal", value: { type: "Text", value: "public" } },
            },
            {
              type: "Exists",
              table: "chat_members",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "chat_id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["__jazz_outer_row", "id"] },
                  },
                  {
                    type: "Cmp",
                    column: "user_id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["user_id"] },
                  },
                ],
              },
            },
          ],
        },
      },
      insert: { with_check: { type: "True" } },
      update: { using: { type: "True" } },
      delete: { using: { type: "True" } },
    },
  },
  chat_members: {
    columns: [
      {
        name: "chat_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "chats",
      },
      { name: "user_id", column_type: { type: "Text" }, nullable: false },
    ],
    policies: {
      select: {
        using: {
          type: "Cmp",
          column: "user_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "user_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
      update: { using: { type: "True" } },
      delete: {
        using: {
          type: "Cmp",
          column: "user_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["user_id"] },
        },
      },
    },
  },
  messages: {
    columns: [
      {
        name: "chat_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "chats",
      },
      { name: "text", column_type: { type: "Text" }, nullable: false },
    ],
    policies: {
      select: {
        using: {
          type: "Or",
          exprs: [
            {
              type: "Exists",
              table: "chats",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["__jazz_outer_row", "chat_id"] },
                  },
                  {
                    type: "Cmp",
                    column: "visibility",
                    op: "Eq",
                    value: { type: "Literal", value: { type: "Text", value: "public" } },
                  },
                ],
              },
            },
            {
              type: "Exists",
              table: "chat_members",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "chat_id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["__jazz_outer_row", "chat_id"] },
                  },
                  {
                    type: "Cmp",
                    column: "user_id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["user_id"] },
                  },
                ],
              },
            },
          ],
        },
      },
      insert: { with_check: { type: "True" } },
      update: { using: { type: "True" } },
      delete: { using: { type: "True" } },
    },
  },
};

describe.skipIf(!hasJazzNapiBuild())("jazz-napi native runtime memory DB", () => {
  let server: LocalJazzServerHandle | null = null;
  const runtimes: NativeRuntimeAdapter[] = [];
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
    const { NapiDb } = await loadNapiModule();
    const wakes: string[] = [];
    const db = NapiDb.openMemory(
      encodeSchema(TEST_SCHEMA),
      openConfig(
        deterministicBytes("jazz-napi-native-runtime:scheduler-node"),
        deterministicBytes("jazz-napi-native-runtime:scheduler-author"),
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

  it("opens, mutates one row, and queries it through the native runtime payload shape", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime:node"),
      deterministicBytes("jazz-napi-native-runtime:author"),
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

  it("delivers native NAPI subscription updates through the native handle", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-subscription:node"),
      deterministicBytes("jazz-napi-native-runtime-subscription:author"),
      21,
      true,
    );
    runtimes.push(runtime);

    const manager = new SubscriptionManager<WasmRow>();
    const updates: ReturnType<SubscriptionManager<WasmRow>["handleDelta"]>[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "local");
    runtime.executeSubscription(handle, (delta: unknown) => {
      updates.push(
        manager.handleDelta(
          delta as Parameters<SubscriptionManager<WasmRow>["handleDelta"]>[0],
          (row) => row,
          TEST_SCHEMA.todos.columns,
        ),
      );
    });

    expect(updates).toEqual([{ all: [], delta: [], reset: true }]);

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi subscribed row" },
      done: { type: "Boolean", value: false },
    });

    expect(updates).toHaveLength(2);
    expect(updates[1]?.delta).toEqual([
      {
        kind: 0,
        id: inserted.id,
        index: 0,
        item: {
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
    expect(updates[2]?.delta).toEqual([
      {
        kind: 2,
        id: inserted.id,
        index: 0,
        item: {
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

  it("applies session ownership policy to local native NAPI inserts and reads", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-policy:node"),
      deterministicBytes("jazz-napi-native-runtime-policy:author"),
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

  it("applies session ownership policy to native NAPI subscriptions", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-policy-subscription:node"),
      deterministicBytes("jazz-napi-native-runtime-policy-subscription:author"),
      14,
      true,
    );
    runtimes.push(runtime);

    const aliceSession = JSON.stringify({ user_id: ALICE_ID });
    const bobSession = JSON.stringify({ user_id: BOB_ID });
    const query = JSON.stringify({ table: "todos" });
    const aliceUpdates: unknown[] = [];
    const decodeAliceDelta = (delta: unknown) =>
      new SubscriptionManager<WasmRow>().handleDelta(
        delta as Parameters<SubscriptionManager<WasmRow>["handleDelta"]>[0],
        (row) => row,
        OWNED_TODOS_SCHEMA.todos.columns,
      );

    const aliceHandle = runtime.createSubscription(query, aliceSession, "local");
    runtime.executeSubscription(aliceHandle, (delta: unknown) => {
      aliceUpdates.push(delta);
    });

    expect(decodeAliceDelta(aliceUpdates[0])).toEqual({ all: [], delta: [], reset: true });

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

    expect(aliceUpdates).toHaveLength(2);
    expect(decodeAliceDelta(aliceUpdates[1])).toEqual(
      expect.objectContaining({
        all: [expect.objectContaining({ id: aliceTodo.id })],
        delta: [
          expect.objectContaining({
            kind: 0,
            id: aliceTodo.id,
            item: expect.objectContaining({
              id: aliceTodo.id,
              values: [
                { type: "Text", value: "alice subscribed row" },
                { type: "Boolean", value: false },
                { type: "Text", value: ALICE_ID },
              ],
            }),
          }),
        ],
      }),
    );
    expect(aliceUpdates).toEqual([
      expect.objectContaining({ __jazzNativeRowDelta: true, addedCount: 0 }),
      expect.objectContaining({
        __jazzNativeRowDelta: true,
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
      }),
    ]);
    expect(decodeAliceDelta(aliceUpdates[1]).delta).toEqual([
      expect.objectContaining({
        kind: 0,
        id: aliceTodo.id,
        item: expect.objectContaining({
          id: aliceTodo.id,
          values: [
            { type: "Text", value: "alice subscribed row" },
            { type: "Boolean", value: false },
            { type: "Text", value: ALICE_ID },
          ],
        }),
      }),
    ]);

    runtime.unsubscribe(aliceHandle);
  });

  it("isolates two session identities sharing one native NAPI runtime for owned deletes", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-delete-policy:node"),
      deterministicBytes("jazz-napi-native-runtime-delete-policy:author"),
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

  it("isolates two session identities sharing one upstream native NAPI runtime for owned deletes", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d003";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "core-napi-owned-delete-admin",
      schema: encodeSchema(OWNED_TODOS_SCHEMA),
    });

    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-edge-delete-policy:node"),
      deterministicBytes("jazz-napi-native-runtime-edge-delete-policy:author"),
      13,
      true,
    );
    runtimes.push(runtime);
    runtime.connect(
      webSocketUrl(server.url, appId),
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

  it("isolates two session identities sharing one persistent upstream native NAPI runtime for owned deletes", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d004";
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-core-owned-delete-"));
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "core-napi-persistent-owned-delete-admin",
      schema: encodeSchema(OWNED_TODOS_SCHEMA),
    });

    try {
      const runtime = new NativeRuntimeAdapter(
        {
          openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDb.openPersistent(path, schema, config) as never,
        },
        OWNED_TODOS_SCHEMA,
        deterministicBytes("jazz-napi-native-runtime-persistent-edge-delete-policy:node"),
        deterministicBytes("jazz-napi-native-runtime-persistent-edge-delete-policy:author"),
        14,
        true,
        { persistentPath: join(tempDir, "db") },
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(server.url, appId),
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

  it("supports native runtime parity writes, mergeable transactions, and upstream transport", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-parity:node"),
      deterministicBytes("jazz-napi-native-runtime-parity:author"),
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

  it("propagates an edge-tier query over the native runtime/server boundary and returns remote row adds", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d001";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "core-napi-edge-query-admin",
      schema: encodeSchema(TEST_SCHEMA),
    });

    const openRuntime = (peer: string, sourceId: number) => {
      const runtime = new NativeRuntimeAdapter(
        { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
        TEST_SCHEMA,
        deterministicBytes(`jazz-napi-core-edge:${peer}:node`),
        deterministicBytes(`jazz-napi-core-edge:${peer}:author`),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(server!.url, appId),
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

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d002";
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-core-server-"));
    server = await startLocalJazzServer({
      appId,
      dataDir: tempDir,
      adminSecret: "core-napi-persistent-edge-query-admin",
      schema: encodeSchema(TEST_SCHEMA),
    });

    const openRuntime = (peer: string, sourceId: number, targetServer: LocalJazzServerHandle) => {
      const runtime = new NativeRuntimeAdapter(
        { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
        TEST_SCHEMA,
        deterministicBytes(`jazz-napi-core-persistent-edge:${peer}:node`),
        deterministicBytes(`jazz-napi-core-persistent-edge:${peer}:author`),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(targetServer.url, appId),
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
        adminSecret: "core-napi-persistent-edge-query-admin",
        schema: encodeSchema(TEST_SCHEMA),
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

  it("propagates session-authenticated branch-policy reads over websocket", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d003";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "core-napi-branch-policy-admin",
      backendSecret: "core-napi-branch-policy-backend",
      schema: encodeSchema(CHAT_POLICY_SCHEMA),
    });

    const openRuntime = (userId: string, sourceId: number) => {
      const runtime = new NativeRuntimeAdapter(
        { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
        CHAT_POLICY_SCHEMA,
        deterministicBytes(`jazz-napi-core-branch-policy:${userId}:node`),
        uuidBytes(userId),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(server!.url, appId),
        JSON.stringify({
          backend_secret: "core-napi-branch-policy-backend",
          backend_session: { user_id: userId, claims: {} },
        }),
      );
      return runtime;
    };

    const writer = openRuntime(ALICE_ID, 51);
    const reader = openRuntime(BOB_ID, 52);
    const inserted = writer.insert("chats", {
      title: { type: "Text", value: "public websocket branch chat" },
      visibility: { type: "Text", value: "public" },
      owner_id: { type: "Text", value: ALICE_ID },
    });

    await waitForPromise(
      writer.waitForTransaction(inserted.transactionId, "edge"),
      "writer public chat insert did not settle at edge",
    );

    const bobSession = JSON.stringify({ user_id: BOB_ID, claims: {} });
    const propagatedRow = await waitFor(async () => {
      const rows = (await reader.query(
        JSON.stringify({ table: "chats" }),
        bobSession,
        "edge",
      )) as Array<{
        id: string;
        table: string;
        values: unknown[];
      }>;
      return rows.find((row) => row.id === inserted.id);
    }, "reader edge query did not receive public branch-policy chat");

    expect(propagatedRow).toEqual({
      id: inserted.id,
      table: "chats",
      values: [
        { type: "Text", value: "public websocket branch chat" },
        { type: "Text", value: "public" },
        { type: "Text", value: ALICE_ID },
      ],
    });

    const message = writer.insert("messages", {
      chat_id: { type: "Uuid", value: inserted.id },
      text: { type: "Text", value: "hello through public chat policy" },
    });
    await waitForPromise(
      writer.waitForTransaction(message.transactionId, "edge"),
      "writer public-chat message insert did not settle at edge",
    );

    const propagatedMessage = await waitFor(async () => {
      const rows = (await reader.query(
        JSON.stringify({ table: "messages" }),
        bobSession,
        "edge",
      )) as Array<{
        id: string;
        table: string;
        values: unknown[];
      }>;
      return rows.find((row) => row.id === message.id);
    }, "reader edge query did not receive message through public-chat branch policy");

    expect(propagatedMessage).toEqual({
      id: message.id,
      table: "messages",
      values: [
        { type: "Uuid", value: inserted.id },
        { type: "Text", value: "hello through public chat policy" },
      ],
    });
  }, 15_000);

  it("reopens a persistent database and reads previously written rows", async () => {
    const { NapiDb } = await loadNapiModule();
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-core-"));
    const dataPath = join(tempDir, "db");
    const node = deterministicBytes("jazz-napi-core-persistent:node");
    const author = deterministicBytes("jazz-napi-core-persistent:author");
    let firstRuntime: NativeRuntimeAdapter | null = null;
    let secondRuntime: NativeRuntimeAdapter | null = null;

    try {
      firstRuntime = new NativeRuntimeAdapter(
        {
          openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDb.openPersistent(path, schema, config) as never,
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

      secondRuntime = new NativeRuntimeAdapter(
        {
          openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDb.openPersistent(path, schema, config) as never,
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

function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) {
    throw new Error(`invalid UUID: ${value}`);
  }
  const bytes = new Uint8Array(16);
  for (let index = 0; index < 16; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
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
