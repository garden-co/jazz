import { afterEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import type { ColumnDescriptor, NativeRowDelta, WasmSchema } from "../../drivers/types.js";
import {
  createRecord,
  type NativeRelationSubscriptionEdge,
  PostcardReader,
  PostcardWriter,
  queryWithPredicates,
  writeDescriptor,
} from "./native-codec.js";
import {
  decodeWebSocketFrameBatch,
  encodeWebSocketPrelude,
  encodeWebSocketFrameBatch,
  isWireHello,
} from "./websocket.js";
import {
  __nativeRuntimeAdapterTestHooks,
  NativeRuntimeAdapter,
  type Transport,
} from "./native-runtime-adapter.js";
import { encodeSchema } from "./schema-codec.js";
import { decodeNativeDelta, SubscriptionManager } from "../subscription-manager.js";
import { definePermissions } from "../../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";
import { createWasmRuntime, hasJazzWasmBuild } from "../testing/wasm-runtime-test-utils.js";

const previousWebSocket = globalThis.WebSocket;

function decodeTestDeltas(
  deltas: unknown[],
  columns: readonly ColumnDescriptor[] = testSchema.todos.columns,
) {
  return deltas.map((delta) => decodeNativeDelta(delta as never, columns));
}

async function waitForServerPumpTimer(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 20));
}

describe("NativeRuntimeAdapter server transport", () => {
  afterEach(() => {
    globalThis.WebSocket = previousWebSocket;
  });

  it("connects the native upstream transport to the scoped websocket endpoint", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([Uint8Array.from([1, 2, 3])]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      Uint8Array.from([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await Promise.resolve();
    await Promise.resolve();
    await waitForServerPumpTimer();

    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.url).toBe("ws://127.0.0.1:4200/apps/app-a/ws");
    expect(sockets[0]!.sent[0]).toEqual(
      encodeWebSocketPrelude(
        "{}",
        Uint8Array.from([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
      ),
    );
    const helloBatch = decodeWebSocketFrameBatch(sockets[0]!.sent[1]! as Uint8Array);
    expect(helloBatch).toHaveLength(1);
    expect(isWireHello(helloBatch[0]!)).toBe(true);
    expect(decodeWebSocketFrameBatch(sockets[0]!.sent[2]! as Uint8Array)).toEqual([
      Uint8Array.from([1, 2, 3]),
    ]);
    expect(transport.closed).toBe(false);

    runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    await Promise.resolve();
    await Promise.resolve();

    expect(sockets).toHaveLength(2);
    expect(sockets[0]!.closed).toBe(true);
    expect(JSON.parse(sockets[1]!.sent[0] as string)).toEqual({
      peer_identity: "01010101010101010101010101010101",
      auth: {
        sub: "01010101010101010101010101010101",
        jwt_token: "fresh.jwt",
      },
      sub: "01010101010101010101010101010101",
      jwt_token: "fresh.jwt",
    });

    runtime.disconnect();

    expect(sockets[1]!.closed).toBe(true);
  });

  it("uses the binding scheduler to drive native db ticks outside server pumps", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([Uint8Array.from([7])]);
    let schedulerCallback: ((urgency: "immediate" | "deferred") => void) | undefined;
    let dbTicks = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => ({
          connectUpstream: () => transport,
          setTickScheduler: (callback: (urgency: "immediate" | "deferred") => void) => {
            schedulerCallback = callback;
          },
          tick: () => {
            dbTicks += 1;
          },
        }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    expect(schedulerCallback).toBeTypeOf("function");

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await Promise.resolve();
    await Promise.resolve();
    await waitForServerPumpTimer();

    expect(transport.tickCount).toBeGreaterThan(0);
    expect(dbTicks).toBe(0);

    schedulerCallback?.("immediate");
    await Promise.resolve();

    expect(transport.tickCount).toBeGreaterThan(1);
    expect(dbTicks).toBe(1);
  });

  it("stages an already-arrived websocket frame group before one native transport tick", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await Promise.resolve();
    await Promise.resolve();
    transport.tickCount = 0;

    const frames = [Uint8Array.from([1]), Uint8Array.from([1, 42]), Uint8Array.from([1, 43])];
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch(frames));
    await Promise.resolve();
    await waitForServerPumpTimer();

    expect(transport.receivedBatches).toEqual([frames]);
    expect(transport.received).toEqual(frames);
    expect(transport.tickCount).toBe(1);
  });

  it("coalesces separate websocket messages that arrive before the server pump timer", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await Promise.resolve();
    await Promise.resolve();
    transport.tickCount = 0;

    const first = Uint8Array.from([1, 10]);
    const second = Uint8Array.from([1, 11]);
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([first]));
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([second]));
    await Promise.resolve();
    await waitForServerPumpTimer();

    expect(transport.receivedBatches).toEqual([[first, second]]);
    expect(transport.received).toEqual([first, second]);
    expect(transport.tickCount).toBe(1);
  });

  it("encodes binary large value columns in native schemas", () => {
    const schemaBytes = encodeSchema({
      files: {
        columns: [
          { name: "inline", column_type: { type: "Bytea" }, nullable: false },
          {
            name: "data",
            column_type: { type: "Bytea" },
            nullable: false,
            large_value: "Blob",
          },
        ],
      },
    });

    expect(readSchemaColumnLargeValues(schemaBytes, "files")).toEqual([
      { name: "inline", largeValue: null },
      { name: "data", largeValue: "Blob" },
    ]);
  });

  it("encodes indexed columns and counter merge strategies in native schemas", () => {
    const schemaBytes = encodeSchema({
      counters: {
        columns: [
          {
            name: "count",
            column_type: { type: "Integer" },
            nullable: false,
            merge_strategy: "Counter",
          },
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
        indexed_columns: ["title", "done"],
      },
    });

    expect(readSchemaTableMetadata(schemaBytes, "counters")).toEqual({
      indexedColumns: ["done", "title"],
      mergeStrategies: [{ column: "count", strategy: "Counter" }],
    });
  });

  it("rejects unsupported native schema merge strategies instead of dropping them", () => {
    expect(() =>
      encodeSchema({
        docs: {
          columns: [
            {
              name: "tags",
              column_type: { type: "Array", element: { type: "Text" } },
              nullable: false,
              merge_strategy: "GSet",
            },
          ],
        },
      }),
    ).toThrow("GSet merge strategies");
  });

  it("resolves connect only after the owned native transport has pumped", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([Uint8Array.from([9])]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await Promise.resolve();
    await Promise.resolve();
    await waitForServerPumpTimer();

    expect(transport.tickCount).toBeGreaterThan(0);
    expect(decodeWebSocketFrameBatch(sockets[0]!.sent[2]! as Uint8Array)).toEqual([
      Uint8Array.from([9]),
    ]);
  });

  it("pumps the newly owned transport before auth-refresh reconnect readiness", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transports = [new FakeTransport([]), new FakeTransport([Uint8Array.from([4, 5, 6])])];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transports.shift()!,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await Promise.resolve();
    await Promise.resolve();
    await runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    await Promise.resolve();
    await Promise.resolve();

    expect(sockets).toHaveLength(2);
    expect(decodeWebSocketFrameBatch(sockets[1]!.sent[2]! as Uint8Array)).toEqual([
      Uint8Array.from([4, 5, 6]),
    ]);
  });

  it("requires native db bindings to expose a tick scheduler", () => {
    expect(
      () =>
        new NativeRuntimeAdapter(
          {
            openMemory: () => ({
              connectUpstream: () => new FakeTransport([]),
              tick: () => undefined,
            }),
            openBrowser: async () => {
              throw new Error("not used");
            },
          } as never,
          testSchema,
          new Uint8Array(16),
          new Uint8Array(16),
          1,
          true,
        ),
    ).toThrow("Native runtime requires db.setTickScheduler");
  });

  it("reports websocket auth failures through the auth failure callback", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const authFailures: string[] = [];
    runtime.onAuthFailure((reason) => authFailures.push(reason));

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await Promise.resolve();

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(3, 1, "token expired")]));
    await Promise.resolve();

    expect(authFailures).toEqual(["expired"]);
    expect(transport.received).toEqual([]);
  });

  it("does not report non-auth websocket errors as auth failures", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const authFailures: string[] = [];
    runtime.onAuthFailure((reason) => authFailures.push(reason));

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await Promise.resolve();

    sockets[0]!.emitMessage(
      encodeWebSocketFrameBatch([encodeWireError(5, 3, "conflicting commit unit")]),
    );
    await Promise.resolve();

    expect(authFailures).toEqual([]);
    expect(transport.received).toEqual([]);
  });

  it("fails active subscriptions when the websocket reports a fatal wire error", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const subscription = {
      closed: false,
      readAll: () => [],
      close() {
        this.closed = true;
        return true;
      },
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            prepareQuery: () => ({}),
            subscribe: () => subscription,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "edge");
    const updates = vi.fn();
    runtime.executeSubscription(handle, updates);
    await Promise.resolve();

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(5, 3, "server died")]));
    await Promise.resolve();

    expect(subscription.closed).toBe(true);
    expect(updates).toHaveBeenCalledTimes(1);
    expect(updates.mock.calls[0]![0]).toBeInstanceOf(Error);
    expect((updates.mock.calls[0]![0] as Error).message).toBe("server died");
    expect(updates.mock.calls[0]![1]).toBeNull();
  });

  it("settle-gates global native subscription chunks before app callbacks", () => {
    const rowId = uuidBytes("00000000-0000-0000-0000-000000000123");
    const events = [
      {
        type: "delta",
        reset: true,
        settled: false,
        delta: encodeSubscriptionDelta({ added: [], updated: [], removed: [] }),
      },
      {
        type: "delta",
        reset: false,
        settled: true,
        delta: encodeSubscriptionDelta({
          added: [{ table: "todos", rowId, title: "settled row" }],
          updated: [],
          removed: [],
        }),
      },
    ];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => ({}),
            subscribe: () => ({
              readAll: () => events.splice(0),
              close: () => true,
            }),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "global");
    const updates = vi.fn();
    runtime.executeSubscription(handle, updates);

    expect(updates).toHaveBeenCalledTimes(1);
    const decoded = decodeTestDeltas([updates.mock.calls[0]![0]]);
    expect(decoded).toHaveLength(1);
    expect(decoded[0]).toHaveLength(1);
    const firstDelta = decoded[0]![0]!;
    expect(firstDelta).toMatchObject({
      kind: 0,
      id: "00000000-0000-0000-0000-000000000123",
      index: 0,
    });
    if (firstDelta.kind !== 0) {
      throw new Error(`expected added delta, got kind ${firstDelta.kind}`);
    }
    expect(firstDelta.row.values[0]).toEqual({ type: "Text", value: "settled row" });
  });

  it("uses the caller-supplied table for update and delete", () => {
    const calls: unknown[] = [];
    const write = {
      payload: new Uint8Array(),
      wait: () => undefined,
      writeState: () => ({}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => Uint8Array.from([0]),
            prepareQuery: () => ({}),
            updateEncoded: (table: string, rowId: Uint8Array, patch: Uint8Array) => {
              calls.push(["update", table, rowId, patch]);
              return write;
            },
            delete: (table: string, rowId: Uint8Array) => {
              calls.push(["delete", table, rowId]);
              return write;
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        todos: {
          columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
        },
        projects: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      },
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    runtime.update("projects", "00000000-0000-0000-0000-000000000001", {
      name: { type: "Text", value: "Project" },
    });
    runtime.delete("projects", "00000000-0000-0000-0000-000000000001");

    expect(calls.map((call) => (call as unknown[]).slice(0, 2))).toEqual([
      ["update", "projects"],
      ["delete", "projects"],
    ]);
  });

  it("serves default and local queries from fresh local state", async () => {
    const insertedRowIds: Uint8Array[] = [];
    const write = {
      payload: new Uint8Array(),
      wait: () => undefined,
      writeState: () => ({}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () =>
              encodeRows([
                {
                  table: "todos",
                  rowId: insertedRowIds[0]!,
                  title: "fresh local write",
                },
              ]),
            prepareQuery: () => ({}),
            insertWithIdEncoded: (_table: string, rowId: Uint8Array) => {
              insertedRowIds.push(rowId);
              return write;
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "fresh local write" },
      },
      null,
      "00000000-0000-0000-0000-000000000000",
    );

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000000",
        values: [{ type: "Text", value: "fresh local write" }],
      },
    ]);
    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "local")).resolves.toEqual(
      [
        {
          table: "todos",
          id: "00000000-0000-0000-0000-000000000000",
          values: [{ type: "Text", value: "fresh local write" }],
        },
      ],
    );
  });

  it("runs scheduled core ticks before post-wait edge reads", async () => {
    let schedulerCallback: ((urgency: "immediate" | "deferred") => void) | undefined;
    let ticked = false;
    let subscriptionDrained = false;
    const rowId = uuidBytes("00000000-0000-0000-0000-000000000123");
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => ({
          all: () =>
            ticked
              ? encodeRows([
                  {
                    table: "todos",
                    rowId,
                    title: "visible after scheduled tick",
                  },
                ])
              : encodeRows([]),
          prepareQuery: () => ({}),
          attachQuery: () => ({}),
          queryAttachmentIsCovered: () => true,
          detachQuery: () => undefined,
          subscribe: () => ({
            readAll: () => {
              if (!ticked || subscriptionDrained) return [];
              subscriptionDrained = true;
              return [
                {
                  type: "snapshot",
                  rows: encodeRelationSnapshot(
                    [
                      {
                        table: "todos",
                        rowId,
                        title: "visible after scheduled tick",
                      },
                    ],
                    [],
                  ),
                },
              ];
            },
          }),
          insertWithIdEncoded: () => {
            schedulerCallback?.("deferred");
            return fakeWrite();
          },
          setTickScheduler: (callback: (urgency: "immediate" | "deferred") => void) => {
            schedulerCallback = callback;
          },
          connectUpstream: () => new FakeTransport([]),
          tick: () => {
            ticked = true;
          },
        }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "edge");
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    const inserted = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "visible after scheduled tick" },
      },
      null,
      "00000000-0000-0000-0000-000000000123",
    );

    await runtime.waitForTransaction(inserted.transactionId, "edge");

    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "edge")).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000123",
        values: [{ type: "Text", value: "visible after scheduled tick" }],
      },
    ]);
    expect(decodeTestDeltas(deltas.slice(0, 2))).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000123",
          row: {
            id: "00000000-0000-0000-0000-000000000123",
            values: [{ type: "Text", value: "visible after scheduled tick" }],
          },
          index: 0,
        },
      ],
    ]);
  });

  it("routes session-scoped queries through allForIdentity", async () => {
    const authors: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              throw new Error("session query should use allForIdentity");
            },
            allForIdentity: (_query: unknown, author: Uint8Array) => {
              authors.push(formatUuidForTest(author));
              return encodeRows([
                {
                  table: "todos",
                  rowId: new Uint8Array(16),
                  title: "session scoped",
                },
              ]);
            },
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({
          user_id: "00000000-0000-0000-0000-0000000000a1",
          claims: {},
          authMode: "anonymous",
        }),
        "local",
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000000",
        values: [{ type: "Text", value: "session scoped" }],
      },
    ]);
    expect(authors).toEqual(["00000000-0000-0000-0000-0000000000a1"]);
  });

  it("stages session-scoped mergeable transaction writes through identity-aware core txs", () => {
    const authors: string[] = [];
    const staged: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeRows([]),
            allForIdentity: () => encodeRows([]),
            mergeableTxForIdentity: (author: Uint8Array) => {
              authors.push(formatUuidForTest(author));
              return fakeTx({
                insertWithIdEncoded: (table: string) => staged.push(table),
              });
            },
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const tx = runtime.beginTransaction("mergeable");
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "session tx" } },
      JSON.stringify({
        batch_id: tx,
        session: { user_id: "00000000-0000-0000-0000-0000000000a1" },
      }),
      "00000000-0000-0000-0000-000000000001",
    );

    expect(authors).toEqual(["00000000-0000-0000-0000-0000000000a1"]);
    expect(staged).toEqual(["todos"]);
  });

  it("passes caller-supplied updatedAt into staged mergeable transaction writes", () => {
    const updatedAt = 1_704_067_200_123_000;
    const expectedUpdatedAtMs = Math.trunc(updatedAt / 1_000);
    const staged: Array<{ op: string; updatedAtMs: number | null | undefined }> = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeRows([]),
            mergeableTx: () =>
              fakeTx({
                insertWithIdEncoded: (_table, _rowId, _cells, updatedAtMs) =>
                  staged.push({ op: "insert", updatedAtMs }),
                updateEncoded: (_table, _rowId, _patch, updatedAtMs) =>
                  staged.push({ op: "update", updatedAtMs }),
                upsertEncoded: (_table, _rowId, _cells, updatedAtMs) =>
                  staged.push({ op: "upsert", updatedAtMs }),
                restoreEncoded: (_table, _rowId, _cells, updatedAtMs) =>
                  staged.push({ op: "restore", updatedAtMs }),
                delete: (_table, _rowId, updatedAtMs) => staged.push({ op: "delete", updatedAtMs }),
              }),
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const tx = runtime.beginTransaction("mergeable");
    const context = JSON.stringify({ batch_id: tx, updated_at: updatedAt });
    const rowId = "00000000-0000-0000-0000-000000000001";
    runtime.insert("todos", { title: { type: "Text", value: "inserted" } }, context, rowId);
    runtime.update("todos", rowId, { title: { type: "Text", value: "updated" } }, context);
    runtime.upsert("todos", rowId, { title: { type: "Text", value: "upserted" } }, context);
    runtime.restore("todos", rowId, { title: { type: "Text", value: "restored" } }, context);
    runtime.delete("todos", rowId, context);

    expect(staged).toEqual([
      { op: "insert", updatedAtMs: expectedUpdatedAtMs },
      { op: "update", updatedAtMs: expectedUpdatedAtMs },
      { op: "upsert", updatedAtMs: expectedUpdatedAtMs },
      { op: "restore", updatedAtMs: expectedUpdatedAtMs },
      { op: "delete", updatedAtMs: expectedUpdatedAtMs },
    ]);
  });

  it("rejects mixed identities within one mergeable transaction", () => {
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeRows([]),
            allForIdentity: () => encodeRows([]),
            mergeableTxForIdentity: () => fakeTx(),
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const tx = runtime.beginTransaction("mergeable");
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "one" } },
      JSON.stringify({
        batch_id: tx,
        session: { user_id: "00000000-0000-0000-0000-0000000000a1" },
      }),
      "00000000-0000-0000-0000-000000000001",
    );

    expect(() =>
      runtime.insert(
        "todos",
        { title: { type: "Text", value: "two" } },
        JSON.stringify({
          batch_id: tx,
          session: { user_id: "00000000-0000-0000-0000-0000000000b2" },
        }),
        "00000000-0000-0000-0000-000000000002",
      ),
    ).toThrow("Native runtime mergeable transaction cannot mix write identities");
  });

  it("routes session-scoped transaction reads through the identity-aware native method", async () => {
    const alice = uuidBytes("00000000-0000-0000-0000-0000000000a1");
    const bob = uuidBytes("00000000-0000-0000-0000-0000000000b2");
    const tx = fakeTx();
    const seenAuthors: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeRows([]),
            allForIdentity: () => encodeRows([]),
            allInTransactionForIdentity: (
              _query: object,
              receivedTx: TxForTest,
              author: Uint8Array,
            ) => {
              expect(receivedTx).toBe(tx);
              seenAuthors.push(formatUuidForTest(author));
              return sameBytesForTest(author, alice)
                ? encodeRows([
                    {
                      table: "todos",
                      rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                      title: "alice pending",
                    },
                  ])
                : encodeRows([]);
            },
            mergeableTxForIdentity: () => tx,
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const transactionId = runtime.beginTransaction("mergeable");
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "alice pending" } },
      JSON.stringify({
        batch_id: transactionId,
        session: { user_id: "00000000-0000-0000-0000-0000000000a1" },
      }),
      "00000000-0000-0000-0000-000000000001",
    );

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({ user_id: "00000000-0000-0000-0000-0000000000b2" }),
        "local",
        JSON.stringify({ transaction_batch_id: transactionId }),
      ),
    ).resolves.toEqual([]);
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({ user_id: "00000000-0000-0000-0000-0000000000a1" }),
        "local",
        JSON.stringify({ transaction_batch_id: transactionId }),
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "alice pending" }],
      },
    ]);
    expect(seenAuthors).toEqual([
      "00000000-0000-0000-0000-0000000000b2",
      "00000000-0000-0000-0000-0000000000a1",
    ]);
  });

  it("decodes fixed-width array columns from native row batches", async () => {
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeBinaryLargeValueRows(),
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      binaryLargeValueSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(runtime.query(JSON.stringify({ table: "binary_large_values" }))).resolves.toEqual([
      {
        table: "binary_large_values",
        id: "00000000-0000-0000-0000-000000000010",
        values: [
          {
            type: "Array",
            value: [
              { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
              { type: "Uuid", value: "00000000-0000-0000-0000-000000000002" },
            ],
          },
          {
            type: "Array",
            value: [
              { type: "Double", value: 65536 },
              { type: "Double", value: 1234 },
            ],
          },
        ],
      },
    ]);
  });

  it("lowers scalar comparison relation IR into the prepared native query", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => new Uint8Array([0]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "todos" } },
            predicate: {
              Cmp: {
                left: { column: "title" },
                op: "Gt",
                right: { Literal: { type: "Text", value: "m" } },
              },
            },
          },
        },
        limit: 5,
      }),
    );

    expect(readPreparedComparison(preparedBytes!)).toEqual({
      table: "todos",
      predicateTag: 6,
      column: "title",
      literalTag: 6,
      value: "m",
      limit: 5,
    });
  });

  it("trusts native prepared queries for simple equality relation filters", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () =>
              encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "keep",
                },
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
                  title: "drop",
                },
              ]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Filter: {
              input: { TableScan: { table: "todos" } },
              predicate: {
                Cmp: {
                  left: { column: "title" },
                  op: "Eq",
                  right: { Literal: { type: "Text", value: "keep" } },
                },
              },
            },
          },
        }),
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "keep" }],
      },
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000002",
        values: [{ type: "Text", value: "drop" }],
      },
    ]);
    expect(readPreparedComparison(preparedBytes!)).toEqual({
      table: "todos",
      predicateTag: 3,
      column: "title",
      literalTag: 6,
      value: "keep",
      limit: undefined,
    });
  });

  it("trusts native subscription snapshots for simple equality relation filters", async () => {
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            subscribe: () =>
              new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              }),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "todos" } },
            predicate: {
              Cmp: {
                left: { column: "title" },
                op: "Eq",
                right: { Literal: { type: "Text", value: "keep" } },
              },
            },
          },
        },
      }),
    );
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue({
      type: "snapshot",
      rows: encodeRelationSnapshot(
        [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
            title: "keep",
          },
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
            title: "drop",
          },
        ],
        [],
      ),
    });
    await Promise.resolve();

    expect(decodeTestDeltas(deltas.slice(0, 2))).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000001",
            values: [{ type: "Text", value: "keep" }],
          },
        },
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000002",
          index: 1,
          row: {
            id: "00000000-0000-0000-0000-000000000002",
            values: [{ type: "Text", value: "drop" }],
          },
        },
      ],
    ]);
    expect(readPreparedComparison(preparedBytes!)).toEqual({
      table: "todos",
      predicateTag: 3,
      column: "title",
      literalTag: 6,
      value: "keep",
      limit: undefined,
    });
  });

  it("routes Join relation IR to the native relation API", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "should not be read",
                },
              ]);
            },
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(JSON.stringify({ table: "todos", relation_ir: unsupportedJoinRelationIr() })),
    ).rejects.toThrow("Native runtime does not support relation queries");
    expect(calls).toEqual([]);
  });

  it("lowers simple Project relation IR while preparing the original subscription query", () => {
    const calls: string[] = [];
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: (query: Uint8Array) => {
              calls.push("prepareQuery");
              preparedBytes = query;
              return {};
            },
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream();
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const handle = runtime.createSubscription(
      JSON.stringify({ table: "todos", relation_ir: unsupportedProjectRelationIr() }),
    );
    expect(handle).toBe(1);
    expect(calls).toEqual(["prepareQuery", "subscribe"]);
    expect(readPreparedSelect(preparedBytes!)).toEqual(["title"]);
  });

  it("subscribes to supported root relation IR as one prepared native query", () => {
    const calls: string[] = [];
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: (query: Uint8Array) => {
              calls.push("prepareQuery");
              preparedBytes = query;
              return {};
            },
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream();
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            { name: "priority", column_type: { type: "Integer" }, nullable: false },
          ],
        },
      },
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        select: ["title"],
        relation_ir: {
          Limit: {
            input: {
              Offset: {
                input: {
                  OrderBy: {
                    input: {
                      Filter: {
                        input: { TableScan: { table: "todos" } },
                        predicate: {
                          Cmp: {
                            left: { column: "title" },
                            op: "Eq",
                            right: { Literal: { type: "Text", value: "native" } },
                          },
                        },
                      },
                    },
                    terms: [{ column: { column: "priority" }, direction: "Desc" }],
                  },
                },
                offset: 2,
              },
            },
            limit: 3,
          },
        },
      }),
    );

    expect(handle).toBe(1);
    expect(calls).toEqual(["prepareQuery", "subscribe"]);
    expect(readPreparedQueryShape(preparedBytes!)).toEqual({
      table: "todos",
      predicates: [{ column: "title", opTag: 3, literalTag: 6, value: "native" }],
      orderBy: [{ column: "priority", directionTag: 1 }],
      limit: 3,
      offset: 2,
    });
    expect(readPreparedSelect(preparedBytes!)).toEqual(["title"]);
  });

  it("encodes public typed-builder root orderBy into native query bytes", () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            subscribe: () => new ReadableStream(),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [["createdAt", "desc"]],
        limit: 10,
      }),
    );

    expect(handle).toBe(1);
    expect(readPreparedQueryShape(preparedBytes!)).toEqual({
      table: "todos",
      predicates: [],
      orderBy: [{ column: "createdAt", directionTag: 1 }],
      limit: 10,
      offset: 0,
    });
  });

  it("encodes negative integer query literals as signed i32 bits for core", () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            subscribe: () => new ReadableStream(),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            { name: "priority", column_type: { type: "Integer" }, nullable: false },
          ],
        },
      },
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "todos" } },
            predicate: {
              Cmp: {
                left: { column: "priority" },
                op: "Lt",
                right: { Literal: { type: "Integer", value: -1 } },
              },
            },
          },
        },
      }),
    );

    expect(readPreparedFirstLiteral(preparedBytes!)).toEqual({
      column: "priority",
      opTag: 8,
      literalTag: 2,
      value: 0x7fffffff,
    });
  });

  it("encodes BIGINT query literals as signed i64 values", () => {
    const query = queryWithPredicates("metrics", [
      { column: "largeCount", op: "Gt", value: { type: "BigInt", value: 9007199254740993n } },
      { column: "largeCount", op: "Lt", value: { type: "BigInt", value: -5n } },
    ]);

    expect(readPreparedComparisonLiterals(query)).toEqual([
      { predicateTag: 6, column: "largeCount", literal: { tag: 13, value: 9007199254740993n } },
      { predicateTag: 8, column: "largeCount", literal: { tag: 13, value: -5n } },
    ]);
  });

  it("materializes array subquery relation snapshots for subscriptions", async () => {
    const calls: string[] = [];
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    const relationSchema = {
      users: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    } satisfies WasmSchema;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              });
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      relationSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "users",
        array_subqueries: [
          {
            column_name: "todosViaOwner",
            table: "todos",
            inner_column: "owner_id",
            outer_column: "id",
          },
        ],
      }),
    );
    expect(handle).toBe(1);

    const deltas: unknown[] = [];
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });
    controller!.enqueue({
      type: "snapshot",
      rows: encodeRelationSnapshot(
        [
          {
            table: "users",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
            title: "Ada",
          },
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
            title: "Ship relation reads",
          },
        ],
        [
          {
            sourceTable: "users",
            sourceRowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
            relation: "todosViaOwner",
            targetTable: "todos",
            targetRowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
          },
        ],
        1,
      ),
    });
    await Promise.resolve();

    expect(calls).toEqual(["prepareQuery", "subscribe"]);
    const relationOutputColumns: ColumnDescriptor[] = [
      relationSchema.users.columns[0]!,
      {
        name: "todosViaOwner",
        column_type: {
          type: "Array",
          element: { type: "Row", columns: relationSchema.todos.columns },
        },
        nullable: false,
      },
    ];
    expect(decodeTestDeltas(deltas, relationOutputColumns)).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000001",
            values: [
              { type: "Text", value: "Ada" },
              {
                type: "Array",
                value: [
                  {
                    type: "Row",
                    value: {
                      id: "00000000-0000-0000-0000-000000000002",
                      values: [{ type: "Text", value: "Ship relation reads" }],
                    },
                  },
                ],
              },
            ],
          },
        },
      ],
    ]);
  });

  it("materializes array subquery relation snapshots for reads", async () => {
    const calls: string[] = [];
    const relationSchema = {
      users: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    } satisfies WasmSchema;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            allRelationSnapshot: () => {
              calls.push("allRelationSnapshot");
              return encodeRelationSnapshot(
                [
                  {
                    table: "users",
                    rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                    title: "Ada",
                  },
                  {
                    table: "todos",
                    rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
                    title: "Ship relation reads",
                  },
                ],
                [
                  {
                    sourceTable: "users",
                    sourceRowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                    relation: "todosViaOwner",
                    targetTable: "todos",
                    targetRowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
                  },
                ],
                1,
              );
            },
            all: () => {
              calls.push("all");
              return encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "should not be read",
                },
              ]);
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      relationSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const rows = (await runtime.query(
      JSON.stringify({
        table: "users",
        array_subqueries: [
          {
            column_name: "todosViaOwner",
            table: "todos",
            inner_column: "owner_id",
            outer_column: "users.id",
          },
        ],
      }),
    )) as Array<{
      table: string;
      id: string;
      values: unknown[];
      valuesByColumn?: Map<string, unknown>;
    }>;

    expect(calls).toEqual(["prepareQuery", "allRelationSnapshot"]);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.table).toBe("users");
    expect(rows[0]?.valuesByColumn?.get("todosViaOwner")).toEqual({
      type: "Array",
      value: [
        {
          type: "Row",
          value: {
            id: "00000000-0000-0000-0000-000000000002",
            values: [{ type: "Text", value: "Ship relation reads" }],
          },
        },
      ],
    });
  });

  it("decodes native subscription chunks", async () => {
    const calls: string[] = [];
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              });
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue({
      type: "snapshot",
      rows: encodeRelationSnapshot(
        [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
            title: "native",
          },
        ],
        [],
      ),
    });
    await Promise.resolve();

    expect(calls).toEqual(["prepareQuery", "subscribe"]);
    expect(decodeTestDeltas(deltas.slice(0, 2))).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000001",
            values: [{ type: "Text", value: "native" }],
          },
        },
      ],
    ]);
  });

  it("rejects Gather subscriptions while preparing the original query", () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream();
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Gather: {
              seed: { TableScan: { table: "todos" } },
              step: {
                Project: {
                  input: {
                    Join: {
                      left: { TableScan: { table: "todos" } },
                      right: { TableScan: { table: "todos" } },
                      on: [{ left: { column: "parent_id" }, right: { column: "id" } }],
                    },
                  },
                },
              },
              bound: { MaxDepth: 3 },
            },
          },
        }),
      ),
    ).toThrow("Native runtime does not support relation query subscriptions");
    expect(calls).toEqual([]);
  });

  it("passes supported read tiers and propagation through native read options", async () => {
    const readOptions: unknown[] = [];
    const attachments: unknown[] = [];
    const detached: unknown[] = [];
    const attachment = {};
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new Uint8Array([0]);
            },
            attachQuery: (_query: unknown, opts: unknown) => {
              attachments.push(opts);
              return attachment;
            },
            queryAttachmentIsCovered: () => true,
            detachQuery: (handle: unknown) => detached.push(handle),
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ propagation: "local-only" }),
      ),
    ).resolves.toEqual([]);

    expect(readOptions).toEqual([{ tier: "edge", propagation: "local_only" }]);
    expect(attachments).toEqual([]);
    expect(detached).toEqual([]);
  });

  it("passes supported read tiers through and fails fast for unsupported read options", async () => {
    const runtime = emptyNativeRuntime();

    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "edge")).resolves.toEqual(
      [],
    );
    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), null, "planetary"),
    ).rejects.toThrow("unsupported read tier");
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "local",
        JSON.stringify({ propagation: "local" }),
      ),
    ).rejects.toThrow("does not support read propagation");
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "local",
        JSON.stringify({ read_view: { source: "branch" } }),
      ),
    ).rejects.toThrow("read_view");
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "local",
        JSON.stringify({ readView: { source: "branch" } }),
      ),
    ).rejects.toThrow("read_view");
  });

  it("passes include_deleted query intent through native read options", async () => {
    const readOptions: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new Uint8Array([0]);
            },
            attachQuery: () => ({}),
            queryAttachmentIsCovered: () => true,
            detachQuery: () => undefined,
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(JSON.stringify({ table: "todos", include_deleted: true }), null, "edge"),
    ).resolves.toEqual([]);

    expect(readOptions).toEqual([{ tier: "edge", include_deleted: true }]);
  });

  it("does not let edge reads run before server query coverage is observed", async () => {
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
    vi.useFakeTimers();
    try {
      const transport = new FakeTransport([]);
      let covered = false;
      let coverageProbeCalls = 0;
      let rowReadCalls = 0;
      const runtime = new NativeRuntimeAdapter(
        {
          openMemory: () =>
            fakeDb({
              all: () => {
                if (!covered) {
                  coverageProbeCalls += 1;
                  throw new Error("NotCovered");
                }
                rowReadCalls += 1;
                return new Uint8Array([0]);
              },
              connectUpstream: () => transport,
              prepareQuery: () => ({}),
              attachQuery: () => ({}),
              queryAttachmentIsCovered: () => covered,
              detachQuery: () => undefined,
              tick: () => undefined,
            }),
          openBrowser: async () => {
            throw new Error("not used");
          },
        } as never,
        testSchema,
        new Uint8Array(16),
        new Uint8Array(16),
        1,
        true,
      );
      await runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");

      const query = runtime.query(JSON.stringify({ table: "todos" }), null, "edge");
      await vi.advanceTimersByTimeAsync(40);

      expect(transport.tickCount).toBeGreaterThan(0);
      expect(coverageProbeCalls).toBeGreaterThan(0);
      expect(rowReadCalls).toBe(0);

      covered = true;
      await vi.advanceTimersByTimeAsync(10);

      await expect(query).resolves.toEqual([]);
      expect(rowReadCalls).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects pending edge reads when the websocket transport errors during coverage wait", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              throw new Error("NotCovered");
            },
            connectUpstream: () => transport,
            prepareQuery: () => ({}),
            attachQuery: () => ({}),
            queryAttachmentIsCovered: () => false,
            detachQuery: () => undefined,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    await runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");

    const query = runtime.query(JSON.stringify({ table: "todos" }), null, "edge");
    await Promise.resolve();
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(4, 3, "server busy")]));

    await expect(query).rejects.toThrow("server busy");
  });

  it("passes supported subscription read tiers through", () => {
    const runtime = emptyNativeRuntime();

    expect(() =>
      runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "edge"),
    ).not.toThrow();
    expect(() =>
      runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "planetary"),
    ).toThrow("unsupported read tier");
  });

  it("rejects include_deleted subscription query intent", () => {
    const runtime = emptyNativeRuntime();

    expect(() =>
      runtime.createSubscription(JSON.stringify({ table: "todos", include_deleted: true })),
    ).toThrow("include_deleted subscriptions");
  });

  it("rejects permission introspection selected columns before preparing flat queries", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return new Uint8Array([0]);
            },
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(JSON.stringify({ table: "todos", select_columns: ["title", "$canRead"] })),
    ).rejects.toThrow("permission-introspection query");
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos", select_columns: ["title", "todos.$canRead"] }),
      ),
    ).rejects.toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("rejects permission introspection predicates before preparing flat queries", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return new Uint8Array([0]);
            },
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          conditions: [{ column: "$canRead", op: "eq", value: true }],
        }),
      ),
    ).rejects.toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("rejects permission introspection in array subqueries before native snapshot prep", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            allRelationSnapshot: () => {
              calls.push("allRelationSnapshot");
              return new Uint8Array([0]);
            },
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          array_subqueries: [
            {
              column_name: "children",
              table: "todos",
              inner_column: "id",
              outer_column: "todos.id",
              select_columns: ["title", "$canRead"],
            },
          ],
        }),
      ),
    ).rejects.toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("rejects permission introspection before subscribing to flat queries", () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream();
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({
          table: "todos",
          conditions: [{ column: "$canRead", op: "eq", value: true }],
          select_columns: ["title", "$canRead"],
        }),
      ),
    ).toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("rejects permission introspection relation projections before native relation APIs", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            allRelationQuery: () => {
              calls.push("allRelationQuery");
              return new Uint8Array([0]);
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Project: {
              input: { TableScan: { table: "todos" } },
              columns: [
                {
                  alias: "$canRead",
                  expr: { Column: { scope: "todos", column: "$canRead" } },
                },
              ],
            },
          },
        }),
      ),
    ).rejects.toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("keeps provenance selected columns on the native flat query path", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "native provenance",
                  createdAt: 42,
                },
              ]);
            },
            prepareQuery: () => {
              calls.push("prepareQuery");
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(JSON.stringify({ table: "todos", select_columns: ["title", "$createdAt"] })),
    ).resolves.toHaveLength(1);
    expect(calls).toEqual(["prepareQuery", "all"]);
  });

  it("passes local-only subscription propagation through native read options", () => {
    const readOptions: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => ({}),
            subscribe: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new ReadableStream();
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ propagation: "local-only" }),
      ),
    ).not.toThrow();

    expect(readOptions).toEqual([{ tier: "edge", propagation: "local_only" }]);
  });

  it("rejects non-default read_view subscription options", () => {
    const runtime = emptyNativeRuntime();

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ read_view: { source: "branch" } }),
      ),
    ).toThrow("read_view");
    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ readView: { source: "branch" } }),
      ),
    ).toThrow("read_view");
  });

  it("accepts well-formed subscription sessions and rejects malformed sessions", () => {
    const runtime = emptyNativeRuntime();

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({ user_id: "00000000-0000-0000-0000-000000000000" }),
      ),
    ).not.toThrow();
    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({ user_id: null }),
      ),
    ).toThrow("session is missing user_id");
  });

  it("applies subscription deltas to the full keyed snapshot", async () => {
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => ({}),
            subscribe: () =>
              new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              }),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue({
      type: "snapshot",
      rows: encodeRelationSnapshot(
        [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
            title: "first",
          },
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
            title: "second",
          },
        ],
        [],
      ),
    });
    await Promise.resolve();

    controller!.enqueue({
      type: "delta",
      delta: encodeSubscriptionDelta({
        added: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000003"),
            title: "third",
            index: 1,
          },
        ],
        updated: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
            title: "second updated",
            index: 0,
          },
        ],
        removed: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
            index: 0,
          },
        ],
      }),
    });
    await Promise.resolve();

    controller!.enqueue({
      type: "snapshot",
      rows: encodeRelationSnapshot([], []),
    });
    await Promise.resolve();

    expect(decodeTestDeltas(deltas.slice(0, 2))).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000001",
            values: [{ type: "Text", value: "first" }],
          },
        },
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000002",
          index: 1,
          row: {
            id: "00000000-0000-0000-0000-000000000002",
            values: [{ type: "Text", value: "second" }],
          },
        },
      ],
      [
        {
          kind: 2,
          id: "00000000-0000-0000-0000-000000000002",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000002",
            values: [{ type: "Text", value: "second updated" }],
          },
        },
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000003",
          index: 1,
          row: {
            id: "00000000-0000-0000-0000-000000000003",
            values: [{ type: "Text", value: "third" }],
          },
        },
        {
          kind: 1,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
        },
      ],
    ]);
    expect(decodeTestDeltas(deltas.slice(2))).toEqual([
      [
        {
          kind: 1,
          id: "00000000-0000-0000-0000-000000000002",
          index: 0,
        },
        {
          kind: 1,
          id: "00000000-0000-0000-0000-000000000003",
          index: 1,
        },
      ],
    ]);
  });

  it("applies core-provided native subscription positions without JS sorting", async () => {
    __nativeRuntimeAdapterTestHooks.reset();
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => ({}),
            subscribe: () =>
              new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              }),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue({
      type: "delta",
      reset: true,
      delta: encodeSubscriptionDelta({
        added: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
            title: "first",
            index: 0,
          },
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000003"),
            title: "third",
            index: 1,
          },
        ],
        updated: [],
        removed: [],
      }),
    });
    await Promise.resolve();

    controller!.enqueue({
      type: "delta",
      reset: false,
      delta: encodeSubscriptionDelta({
        added: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
            title: "second",
            index: 1,
          },
        ],
        updated: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000003"),
            title: "third updated",
            index: 2,
          },
        ],
        removed: [],
      }),
    });
    await Promise.resolve();

    const manager = new SubscriptionManager<{ id: string; title: string }>();
    manager.handleDelta(
      deltas[0] as NativeRowDelta,
      (row) => ({
        id: row.id,
        title: row.values[0]?.type === "Text" ? row.values[0].value : "",
      }),
      testSchema.todos.columns,
    );
    const reduced = manager.handleDelta(
      deltas[1] as NativeRowDelta,
      (row) => ({
        id: row.id,
        title: row.values[0]?.type === "Text" ? row.values[0].value : "",
      }),
      testSchema.todos.columns,
    );
    expect(reduced.all?.map((row) => row.id)).toEqual([
      "00000000-0000-0000-0000-000000000001",
      "00000000-0000-0000-0000-000000000002",
      "00000000-0000-0000-0000-000000000003",
    ]);
    expect(__nativeRuntimeAdapterTestHooks.legacySemanticDeltaCalls).toBe(0);
  });

  it("does not run db.all snapshot refresh cycles for maintained native subscriptions", async () => {
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    let allCalls = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              allCalls += 1;
              return encodeRows([]);
            },
            prepareQuery: () => ({}),
            insertWithIdEncoded: () => fakeWrite(),
            subscribe: () =>
              new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              }),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    const updates = vi.fn();
    runtime.executeSubscription(handle, updates);

    controller!.enqueue({
      type: "delta",
      reset: true,
      settled: true,
      delta: encodeSubscriptionDelta({
        added: [],
        updated: [],
        removed: [],
      }),
    });
    await flushAdapterMicrotasks();
    expect(updates).toHaveBeenCalledTimes(1);

    for (let index = 0; index < 5; index += 1) {
      runtime.insert(
        "todos",
        { title: { type: "Text", value: `row-${index}` } },
        null,
        `00000000-0000-0000-0000-${String(index + 1).padStart(12, "0")}`,
      );
      controller!.enqueue({
        type: "delta",
        reset: false,
        settled: true,
        delta: encodeSubscriptionDelta({
          added: [
            {
              table: "todos",
              rowId: indexedUuidBytes(index + 1),
              title: `row-${index}`,
              index,
            },
          ],
          updated: [],
          removed: [],
        }),
      });
      await flushAdapterMicrotasks();
    }

    expect(allCalls).toBe(0);
  });

  it("encodes public id equality relation filters into prepared native queries", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () =>
              encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "native returned requested",
                },
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
                  title: "native returned extra",
                },
              ]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Filter: {
              input: { TableScan: { table: "todos" } },
              predicate: {
                Cmp: {
                  left: { column: "id" },
                  op: "Eq",
                  right: {
                    Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
                  },
                },
              },
            },
          },
        }),
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "native returned requested" }],
      },
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000002",
        values: [{ type: "Text", value: "native returned extra" }],
      },
    ]);
    expect(readPreparedUuidComparison(preparedBytes!)).toEqual({
      table: "todos",
      predicateTag: 3,
      column: "id",
      literalTag: 8,
      value: "00000000-0000-0000-0000-000000000001",
      limit: undefined,
    });
  });

  it("preserves raw provenance timestamps from native rows without Date.now fallbacks", async () => {
    const createdAtMs = 42;
    const updatedAtMs = 43;
    const rowId = "00000000-0000-0000-0000-000000000001";
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () =>
              encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes(rowId),
                  title: "native provenance",
                  createdAt: createdAtMs,
                  updatedAt: updatedAtMs,
                },
              ]),
            prepareQuery: () => ({}),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    const [row] = (await runtime.query(
      JSON.stringify({
        table: "todos",
        select_columns: ["title", "$createdAt", "$updatedAt"],
        relation_ir: { TableScan: { table: "todos" } },
      }),
    )) as Array<{ valuesByColumn?: Map<string, { type: string; value: number }> }>;

    expect(row?.valuesByColumn?.get("$createdAt")).toEqual({
      type: "Timestamp",
      value: createdAtMs * 1_000,
    });
    expect(row?.valuesByColumn?.get("$updatedAt")).toEqual({
      type: "Timestamp",
      value: updatedAtMs * 1_000,
    });
  });

  it("encodes public id in conditions into prepared native queries", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => new Uint8Array([0]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "id",
            op: "in",
            value: ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"],
          },
        ],
      }),
    );

    expect(readPreparedUuidIn(preparedBytes!)).toEqual({
      table: "todos",
      column: "id",
      values: ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"],
    });
  });

  it("encodes uuid-looking condition values as text for text columns", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => new Uint8Array([0]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "title",
            op: "eq",
            value: "00000000-0000-0000-0000-000000000001",
          },
        ],
      }),
    );

    expect(readPreparedComparison(preparedBytes!)).toEqual({
      table: "todos",
      predicateTag: 3,
      column: "title",
      literalTag: 6,
      value: "00000000-0000-0000-0000-000000000001",
      limit: undefined,
    });
  });

  it("preserves relation IR in literals for numeric and timestamp columns", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => new Uint8Array([0]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        metrics: {
          columns: [
            { name: "count", column_type: { type: "Integer" }, nullable: false },
            { name: "ratio", column_type: { type: "Double" }, nullable: false },
            { name: "createdAt", column_type: { type: "Timestamp" }, nullable: false },
          ],
        },
      } satisfies WasmSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "metrics",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "metrics" } },
            predicate: {
              And: [
                {
                  In: {
                    left: { column: "count" },
                    values: [
                      { Literal: { type: "Integer", value: 5 } },
                      { Literal: { type: "Integer", value: 10 } },
                    ],
                  },
                },
                {
                  In: {
                    left: { column: "ratio" },
                    values: [
                      { Literal: { type: "Double", value: 1.5 } },
                      { Literal: { type: "Double", value: 2.5 } },
                    ],
                  },
                },
                {
                  In: {
                    left: { column: "createdAt" },
                    values: [
                      { Literal: { type: "Timestamp", value: 1767225600000 } },
                      { Literal: { type: "Timestamp", value: 1767312000000 } },
                    ],
                  },
                },
              ],
            },
          },
        },
      }),
    );

    expect(readPreparedInLiterals(preparedBytes!)).toEqual([
      {
        column: "count",
        literals: [
          { tag: 2, value: encodeSignedI32ForTest(5) },
          { tag: 2, value: encodeSignedI32ForTest(10) },
        ],
      },
      {
        column: "ratio",
        literals: [
          { tag: 4, value: 1.5 },
          { tag: 4, value: 2.5 },
        ],
      },
      {
        column: "createdAt",
        literals: [
          { tag: 3, value: 1767225600000 },
          { tag: 3, value: 1767312000000 },
        ],
      },
    ]);
  });

  it("preserves relation IR range literal types for double and timestamp columns", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => new Uint8Array([0]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        metrics: {
          columns: [
            { name: "ratio", column_type: { type: "Double" }, nullable: false },
            { name: "createdAt", column_type: { type: "Timestamp" }, nullable: false },
          ],
        },
      } satisfies WasmSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "metrics",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "metrics" } },
            predicate: {
              And: [
                {
                  Cmp: {
                    left: { column: "ratio" },
                    op: "Gt",
                    right: { Literal: { type: "Double", value: 1.5 } },
                  },
                },
                {
                  Cmp: {
                    left: { column: "ratio" },
                    op: "Lt",
                    right: { Literal: { type: "Double", value: 4.5 } },
                  },
                },
                {
                  Cmp: {
                    left: { column: "createdAt" },
                    op: "Gt",
                    right: { Literal: { type: "Timestamp", value: 1770076800000 } },
                  },
                },
                {
                  Cmp: {
                    left: { column: "createdAt" },
                    op: "Lt",
                    right: { Literal: { type: "Timestamp", value: 1770336000000 } },
                  },
                },
              ],
            },
          },
        },
      }),
    );

    expect(readPreparedComparisonLiterals(preparedBytes!)).toEqual([
      { predicateTag: 6, column: "ratio", literal: { tag: 4, value: 1.5 } },
      { predicateTag: 8, column: "ratio", literal: { tag: 4, value: 4.5 } },
      { predicateTag: 6, column: "createdAt", literal: { tag: 3, value: 1770076800000 } },
      { predicateTag: 8, column: "createdAt", literal: { tag: 3, value: 1770336000000 } },
    ]);
  });

  it("does not filter native subscription snapshots by public id in JS", async () => {
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            subscribe: () =>
              new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              }),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "todos" } },
            predicate: {
              Cmp: {
                left: { column: "id" },
                op: "Eq",
                right: {
                  Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
                },
              },
            },
          },
        },
      }),
    );
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue({
      type: "snapshot",
      rows: encodeRelationSnapshot(
        [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
            title: "requested",
          },
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
            title: "extra from native",
          },
        ],
        [],
      ),
    });
    await Promise.resolve();

    expect(decodeTestDeltas(deltas)[0]).toHaveLength(2);
    expect(readPreparedUuidComparison(preparedBytes!)).toMatchObject({
      table: "todos",
      predicateTag: 3,
      column: "id",
      literalTag: 8,
      value: "00000000-0000-0000-0000-000000000001",
    });
  });

  it("delivers packed reset rows with the same public shape as legacy decode when native batches include internal fields", () => {
    const chunk = {
      type: "delta",
      reset: true,
      settled: true,
      delta: encodeSubscriptionDelta({
        added: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000123"),
            title: "packed reset public row",
            txTime: 123,
          },
        ],
        updated: [],
        removed: [],
      }),
      relation_delta: encodeRelationSubscriptionDelta({
        baseCursor: 0,
        cursor: 1,
        added: [],
        updated: [],
        removed: [],
        addedEdges: [],
        removedEdges: [],
      }),
    };
    const runtime = runtimeWithNativeSubscriptionChunk(chunk);
    const deltas: NativeRowDelta[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, null, null);

    runtime.executeSubscription(handle, (delta: NativeRowDelta) => {
      deltas.push(delta);
    });

    expect(deltas).toHaveLength(1);
    const decoded = decodeNativeDelta(deltas[0]!, testSchema.todos.columns);
    expect(decoded).toEqual([
      {
        kind: 0,
        id: "00000000-0000-0000-0000-000000000123",
        index: 0,
        row: {
          id: "00000000-0000-0000-0000-000000000123",
          values: [{ type: "Text", value: "packed reset public row" }],
        },
      },
    ]);
    expect(decoded[0]?.kind).toBe(0);
    if (decoded[0]?.kind !== 0) throw new Error("expected added row");
    expect(Object.keys(decoded[0].row)).toEqual(["id", "values"]);
    runtime.close();
  });

  it("rewraps user field option bytes when packed reset frames filter engine records", () => {
    const schema = {
      notes: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "note", column_type: { type: "Text" }, nullable: true },
        ],
      },
    } satisfies WasmSchema;
    const chunk = {
      type: "delta",
      reset: true,
      settled: true,
      delta: encodeUserWrappedSubscriptionDelta({
        table: "notes",
        rowId: uuidBytes("00000000-0000-0000-0000-000000000321"),
        title: "plain public title",
        note: "nullable public note",
      }),
      relation_delta: encodeRelationSubscriptionDelta({
        baseCursor: 0,
        cursor: 1,
        added: [],
        updated: [],
        removed: [],
        addedEdges: [],
        removedEdges: [],
      }),
    };
    const runtime = runtimeWithNativeSubscriptionChunk(chunk, schema);
    const deltas: NativeRowDelta[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "notes" }), null, null, null);

    runtime.executeSubscription(handle, (delta: NativeRowDelta) => {
      deltas.push(delta);
    });

    expect(deltas).toHaveLength(1);
    const decoded = decodeNativeDelta(deltas[0]!, schema.notes.columns);
    expect(decoded).toEqual([
      {
        kind: 0,
        id: "00000000-0000-0000-0000-000000000321",
        index: 0,
        row: {
          id: "00000000-0000-0000-0000-000000000321",
          values: [
            { type: "Text", value: "plain public title" },
            { type: "Text", value: "nullable public note" },
          ],
        },
      },
    ]);
    runtime.close();
  });

  it("encodes range id comparisons into prepared native queries", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () =>
              encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "drop",
                },
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
                  title: "keep",
                },
              ]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Filter: {
              input: { TableScan: { table: "todos" } },
              predicate: {
                Cmp: {
                  left: { column: "id" },
                  op: "Gt",
                  right: {
                    Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
                  },
                },
              },
            },
          },
        }),
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "drop" }],
      },
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000002",
        values: [{ type: "Text", value: "keep" }],
      },
    ]);
    expect(readPreparedUuidComparison(preparedBytes!)).toMatchObject({
      table: "todos",
      predicateTag: 6,
      column: "id",
      literalTag: 8,
      value: "00000000-0000-0000-0000-000000000001",
    });
  });

  it("pushes limits with native id predicates", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => new Uint8Array([0]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Limit: {
            input: {
              Filter: {
                input: { TableScan: { table: "todos" } },
                predicate: {
                  Cmp: {
                    left: { column: "id" },
                    op: "Eq",
                    right: {
                      Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
                    },
                  },
                },
              },
            },
            limit: 1,
          },
        },
      }),
    );

    expect(readPreparedLimit(preparedBytes!)).toBe(1);
  });

  it("lowers root order and pagination into the prepared core query", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => new Uint8Array([0]),
            prepareQuery: (query: Uint8Array) => {
              preparedBytes = query;
              return {};
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            { name: "priority", column_type: { type: "Integer" }, nullable: false },
          ],
        },
      },
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Limit: {
            input: {
              Offset: {
                input: {
                  OrderBy: {
                    input: {
                      Filter: {
                        input: { TableScan: { table: "todos" } },
                        predicate: {
                          Cmp: {
                            left: { column: "title" },
                            op: "Eq",
                            right: { Literal: { type: "Text", value: "ship it" } },
                          },
                        },
                      },
                    },
                    terms: [
                      { column: { column: "priority" }, direction: "Desc" },
                      { column: { column: "title" }, direction: "Asc" },
                    ],
                  },
                },
                offset: 5,
              },
            },
            limit: 10,
          },
        },
      }),
    );

    expect(readPreparedQueryShape(preparedBytes!)).toEqual({
      table: "todos",
      predicates: [{ column: "title", opTag: 3, literalTag: 6, value: "ship it" }],
      orderBy: [
        { column: "priority", directionTag: 1 },
        { column: "title", directionTag: 0 },
      ],
      limit: 10,
      offset: 5,
    });
  });

  it("serializes OR policy branches with Exists source id correlation", () => {
    const policy = readSchemaSelectPolicyBranches(
      encodeSchema({
        documents: {
          columns: [
            { name: "visibility", column_type: { type: "Text" }, nullable: false },
            { name: "title", column_type: { type: "Text" }, nullable: false },
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
                    table: "document_members",
                    condition: {
                      type: "And",
                      exprs: [
                        {
                          type: "Cmp",
                          column: "document_id",
                          op: "Eq",
                          value: {
                            type: "SessionRef",
                            path: ["__jazz_outer_row", "id"],
                          },
                        },
                        {
                          type: "Cmp",
                          column: "role",
                          op: "Eq",
                          value: { type: "Literal", value: { type: "Text", value: "reader" } },
                        },
                      ],
                    },
                  },
                ],
              },
            },
          },
        },
        document_members: {
          columns: [
            { name: "document_id", column_type: { type: "Uuid" }, nullable: false },
            { name: "role", column_type: { type: "Text" }, nullable: false },
          ],
        },
      }),
      "documents",
    );

    expect(policy).toEqual({
      table: "documents",
      filters: [{ tag: 1, children: [] }],
      joins: [],
      branches: [
        {
          filters: [
            {
              tag: 3,
              column: "visibility",
              operand: { tag: 3, literalTag: 6, value: "public" },
            },
          ],
          joins: [],
        },
        {
          filters: [],
          joins: [
            {
              table: "document_members",
              onColumn: "document_id",
              targetTag: 0,
              sourceColumn: "id",
              sourceLookup: undefined,
              filters: [
                {
                  tag: 3,
                  column: "role",
                  operand: { tag: 3, literalTag: 6, value: "reader" },
                },
              ],
              nestedJoins: [],
            },
          ],
        },
      ],
    });
  });

  it("serializes same-table gather seeds as seeded reachable policies", () => {
    const reachables = readSchemaSelectPolicyReachables(
      encodeSchema({
        resources: {
          columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
          policies: {
            select: {
              using: {
                type: "ExistsRel",
                rel: {
                  Filter: {
                    input: {
                      Join: {
                        left: {
                          Gather: {
                            seed: {
                              Filter: {
                                input: { TableScan: { table: "teams" } },
                                predicate: {
                                  Cmp: {
                                    left: { scope: "teams", column: "identity_key" },
                                    op: "Eq",
                                    right: { SessionRef: ["userId"] },
                                  },
                                },
                              },
                            },
                            step: {
                              Project: {
                                input: {
                                  Join: {
                                    left: {
                                      Filter: {
                                        input: { TableScan: { table: "team_entries" } },
                                        predicate: {
                                          And: [
                                            {
                                              Cmp: {
                                                left: {
                                                  scope: "team_entries",
                                                  column: "member_id",
                                                },
                                                op: "Eq",
                                                right: { RowId: "Frontier" },
                                              },
                                            },
                                            {
                                              Cmp: {
                                                left: {
                                                  scope: "team_entries",
                                                  column: "administrator",
                                                },
                                                op: "Eq",
                                                right: {
                                                  Literal: { type: "Boolean", value: false },
                                                },
                                              },
                                            },
                                          ],
                                        },
                                      },
                                    },
                                    right: {
                                      TableScan: {
                                        table: "teams",
                                        alias: "__recursive_hop_0",
                                      },
                                    },
                                    on: [
                                      {
                                        left: { scope: "team_entries", column: "target_id" },
                                        right: { scope: "__recursive_hop_0", column: "id" },
                                      },
                                    ],
                                    join_kind: "Inner",
                                  },
                                },
                                columns: [],
                              },
                            },
                            frontier_key: { RowId: "Current" },
                            bound: { MaxDepth: 8 },
                            dedupe_key: [{ RowId: "Current" }],
                          },
                        },
                        right: { TableScan: { table: "resource_access", alias: "access" } },
                        on: [
                          {
                            left: { column: "id" },
                            right: { scope: "access", column: "team" },
                          },
                        ],
                        join_kind: "Inner",
                      },
                    },
                    predicate: {
                      And: [
                        {
                          Cmp: {
                            left: { scope: "access", column: "resource" },
                            op: "Eq",
                            right: { RowId: "Outer" },
                          },
                        },
                        {
                          Cmp: {
                            left: { scope: "access", column: "administrator" },
                            op: "Eq",
                            right: { Literal: { type: "Boolean", value: false } },
                          },
                        },
                      ],
                    },
                  },
                },
              },
            },
          },
        },
        teams: {
          columns: [{ name: "identity_key", column_type: { type: "Uuid" }, nullable: false }],
        },
        team_entries: {
          columns: [
            {
              name: "member_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
            {
              name: "target_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
            { name: "administrator", column_type: { type: "Boolean" }, nullable: false },
          ],
        },
        resource_access: {
          columns: [
            {
              name: "resource",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "resources",
            },
            {
              name: "team",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
            { name: "administrator", column_type: { type: "Boolean" }, nullable: false },
          ],
        },
      }),
      "resources",
    );

    expect(reachables).toHaveLength(1);
    expect(reachables[0]).toMatchObject({
      accessTable: "resource_access",
      accessRowColumn: "resource",
      accessTeamColumn: "team",
      accessTeamTargetTag: 0,
      edgeTable: "team_entries",
      edgeMemberColumn: "member_id",
      edgeParentColumn: "target_id",
      maxDepth: 8,
      seed: {
        table: "teams",
        userColumn: "identity_key",
        userClaim: "user_id",
        teamColumn: "id",
        filters: [],
      },
    });
    expect(reachables[0]!.accessFilters).toEqual([
      {
        tag: 3,
        column: "administrator",
        operand: { tag: 3, literalTag: 5, value: false },
      },
    ]);
    expect(reachables[0]!.edgeFilters).toEqual([
      {
        tag: 3,
        column: "administrator",
        operand: { tag: 3, literalTag: 5, value: false },
      },
    ]);
  });

  it("serializes reachable_via seeded_by TS policies as the Rust reachable atom", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
      },
      teams: {
        columns: [{ name: "identity_key", column_type: { type: "Text" }, nullable: false }],
      },
      team_team_edges: {
        columns: [
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
      resource_access_edges: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
          {
            name: "team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          { name: "grant_role", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      teams: { _rowType: {} as never, where: (_input: unknown) => undefined },
      team_team_edges: { _rowType: {} as never, where: (_input: unknown) => undefined },
      resource_access_edges: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.resources.allowRead.where(
        policy.exists(
          policy.resources
            .reachable_via_with_access_filters(
              "resource_access_edges",
              "resource",
              "team",
              session.sub,
              { grant_role: "viewer" },
              "team_team_edges",
              "child_team",
              "parent_team",
            )
            .seeded_by("teams", "identity_key", "sub", "id"),
        ),
      );
    });
    const reachables = readSchemaSelectPolicyReachables(
      encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions)),
      "resources",
    );

    expect(reachables).toHaveLength(1);
    expect(reachables[0]).toMatchObject({
      accessTable: "resource_access_edges",
      accessRowColumn: "resource",
      accessTeamColumn: "team",
      accessTeamTargetTag: 0,
      edgeTable: "team_team_edges",
      edgeMemberColumn: "child_team",
      edgeParentColumn: "parent_team",
      maxDepth: 8,
      seed: {
        table: "teams",
        userColumn: "identity_key",
        userClaim: "sub",
        teamColumn: "id",
        filters: [],
      },
    });
    expect(reachables[0]!.accessFilters).toEqual([
      {
        tag: 3,
        column: "grant_role",
        operand: { tag: 3, literalTag: 6, value: "viewer" },
      },
    ]);
  });

  it("serializes allowedTo.read as a native inherits policy atom", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
      },
      data_entries: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
          { name: "label", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      data_entries: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, allowedTo }) => {
      policy.resources.allowRead.where({ label: "visible" });
      policy.data_entries.allowRead.where(allowedTo.read("resource"));
    });

    const policy = readSchemaSelectPolicyInherits(
      encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions)),
      "data_entries",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
  });

  it("serializes allowedTo insert, update, and delete as native inherits policy atoms", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
      },
      data_entries: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
          { name: "label", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      data_entries: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, allowedTo }) => {
      policy.data_entries.allowInsert.where(allowedTo.insert("resource"));
      policy.data_entries.allowUpdate
        .whereOld(allowedTo.update("resource"))
        .whereNew(allowedTo.update("resource"));
      policy.data_entries.allowDelete.where(allowedTo.delete("resource"));
    });

    const encoded = encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions));

    expect(readSchemaPolicyInherits(encoded, "data_entries", "insert")).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
    expect(readSchemaPolicyInherits(encoded, "data_entries", "updateUsing")).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
    expect(readSchemaPolicyInherits(encoded, "data_entries", "updateCheck")).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
    expect(readSchemaPolicyInherits(encoded, "data_entries", "delete")).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
  });

  it("serializes authored inherited policies byte-identically to native schema atoms", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
        policies: {
          select: { using: { type: "True" } },
          insert: { with_check: { type: "True" } },
          update: { using: { type: "True" }, with_check: { type: "True" } },
          delete: { using: { type: "True" } },
        },
      },
      data_entries: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
          { name: "label", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      data_entries: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, allowedTo }) => {
      policy.data_entries.allowRead.where(allowedTo.read("resource"));
      policy.data_entries.allowInsert.where(allowedTo.insert("resource"));
      policy.data_entries.allowUpdate
        .whereOld(allowedTo.update("resource"))
        .whereNew(allowedTo.update("resource"));
      policy.data_entries.allowDelete.where(allowedTo.delete("resource"));
    });
    const nativeSchema: WasmSchema = {
      ...baseSchema,
      data_entries: {
        ...baseSchema.data_entries,
        policies: {
          select: {
            using: { type: "Inherits", operation: "Select", via_column: "resource" },
          },
          insert: {
            with_check: { type: "Inherits", operation: "Insert", via_column: "resource" },
          },
          update: {
            using: { type: "Inherits", operation: "Update", via_column: "resource" },
            with_check: { type: "Inherits", operation: "Update", via_column: "resource" },
          },
          delete: {
            using: { type: "Inherits", operation: "Delete", via_column: "resource" },
          },
        },
      },
    };

    expect(encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions))).toEqual(
      encodeSchema(nativeSchema),
    );
  });

  it("encodes the policy graph perf fixture byte-stably", () => {
    const fixtureDir = new URL("../../testing/fixtures/policy-graph-perf/", import.meta.url);
    const source = JSON.parse(readFileSync(new URL("schema-source.json", fixtureDir), "utf8")) as {
      mergedSchema: WasmSchema;
    };
    const expectedBytes = new Uint8Array(readFileSync(new URL("schema.native.bin", fixtureDir)));
    const encoded = encodeSchema(source.mergedSchema);

    expect(encoded).toEqual(expectedBytes);
  });

  it("rejects ExistsRel Gather policies without a concrete MaxDepth bound", () => {
    expect(() =>
      encodeSchema({
        teams: {
          columns: [
            {
              name: "parent_id",
              column_type: { type: "Uuid" },
              nullable: true,
              references: "teams",
            },
          ],
          policies: {
            select: {
              using: {
                type: "ExistsRel",
                rel: {
                  Gather: {
                    seed: {
                      Project: {
                        input: {
                          Filter: {
                            input: {
                              Join: {
                                left: { TableScan: { table: "teams", alias: "edge" } },
                                right: { TableScan: { table: "teams", alias: "seed" } },
                                on: [
                                  {
                                    left: { scope: "edge", column: "parent_id" },
                                    right: { scope: "seed", column: "id" },
                                  },
                                ],
                                join_kind: "Inner",
                              },
                            },
                            predicate: {
                              Cmp: {
                                left: { scope: "seed", column: "parent_id" },
                                op: "Eq",
                                right: { SessionRef: ["teamId"] },
                              },
                            },
                          },
                        },
                        columns: [],
                      },
                    },
                    step: {
                      Project: {
                        input: {
                          Join: {
                            left: {
                              Filter: {
                                input: { TableScan: { table: "teams", alias: "edge" } },
                                predicate: {
                                  Cmp: {
                                    left: { scope: "edge", column: "id" },
                                    op: "Eq",
                                    right: { RowId: "Frontier" },
                                  },
                                },
                              },
                            },
                            right: { TableScan: { table: "teams", alias: "next" } },
                            on: [
                              {
                                left: { scope: "edge", column: "parent_id" },
                                right: { scope: "next", column: "id" },
                              },
                            ],
                            join_kind: "Inner",
                          },
                        },
                        columns: [],
                      },
                    },
                    frontier_key: { RowId: "Frontier" },
                    bound: "Fixpoint",
                    dedupe_key: [{ RowId: "Current" }],
                  },
                },
              } as never,
            },
          },
        },
      }),
    ).toThrow("MaxDepth");
  });

  it("serializes InheritsReferencing without a source operation policy as fail-closed", () => {
    const policy = readSchemaSelectPolicyBranches(
      encodeSchema({
        projects: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
          policies: {
            select: {
              using: {
                type: "InheritsReferencing",
                operation: "Select",
                source_table: "todos",
                via_column: "project_id",
              },
            },
          },
        },
        todos: {
          columns: [
            {
              name: "project_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "projects",
            },
            { name: "title", column_type: { type: "Text" }, nullable: false },
          ],
        },
      }),
      "projects",
    );

    expect(policy).toEqual({
      table: "projects",
      filters: [],
      joins: [
        {
          table: "todos",
          onColumn: "project_id",
          targetTag: 0,
          sourceColumn: undefined,
          sourceLookup: undefined,
          filters: [{ tag: 1, children: [] }],
          nestedJoins: [],
        },
      ],
      branches: [],
    });
  });

  it("serializes direct Inherits delete as a native inherits policy atom", () => {
    const policy = readSchemaPolicyInherits(
      encodeSchema({
        messages: {
          columns: [{ name: "room_id", column_type: { type: "Uuid" }, nullable: false }],
          policies: {
            delete: {
              using: {
                type: "Cmp",
                column: "room_id",
                op: "Eq",
                value: { type: "SessionRef", path: ["roomId"] },
              },
            },
          },
        },
        reactions: {
          columns: [
            {
              name: "message_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "messages",
            },
          ],
          policies: {
            delete: {
              using: {
                type: "Inherits",
                operation: "Delete",
                via_column: "message_id",
              },
            },
          },
        },
      }),
      "reactions",
      "delete",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "message_id" }],
      joinCount: 0,
    });
  });

  it("serializes direct Inherits through parent exists joins with source lookup", () => {
    const policy = readSchemaSelectPolicyInherits(
      encodeSchema({
        chats: {
          columns: [{ name: "isPublic", column_type: { type: "Boolean" }, nullable: false }],
          policies: {
            select: {
              using: {
                type: "Exists",
                table: "chatMembers",
                condition: {
                  type: "And",
                  exprs: [
                    {
                      type: "Cmp",
                      column: "chatId",
                      op: "Eq",
                      value: { type: "OuterRowRef", column: "id" } as never,
                    },
                    {
                      type: "Cmp",
                      column: "userId",
                      op: "Eq",
                      value: { type: "SessionRef", path: ["user_id"] },
                    },
                  ],
                },
              },
            },
          },
        },
        chatMembers: {
          columns: [
            {
              name: "chatId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "chats",
            },
            { name: "userId", column_type: { type: "Text" }, nullable: false },
          ],
        },
        canvases: {
          columns: [
            {
              name: "chatId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "chats",
            },
          ],
          policies: {
            select: {
              using: {
                type: "Inherits",
                operation: "Select",
                via_column: "chatId",
              },
            },
          },
        },
      }),
      "canvases",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "chatId" }],
      joinCount: 0,
    });
  });

  it("serializes nested Inherits through composed source lookups", () => {
    const policy = readSchemaSelectPolicyInherits(
      encodeSchema({
        chats: {
          columns: [{ name: "isPublic", column_type: { type: "Boolean" }, nullable: false }],
          policies: {
            select: {
              using: {
                type: "Exists",
                table: "chatMembers",
                condition: {
                  type: "And",
                  exprs: [
                    {
                      type: "Cmp",
                      column: "chatId",
                      op: "Eq",
                      value: { type: "OuterRowRef", column: "id" } as never,
                    },
                    {
                      type: "Cmp",
                      column: "userId",
                      op: "Eq",
                      value: { type: "SessionRef", path: ["user_id"] },
                    },
                  ],
                },
              },
            },
          },
        },
        chatMembers: {
          columns: [
            {
              name: "chatId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "chats",
            },
            { name: "userId", column_type: { type: "Text" }, nullable: false },
          ],
        },
        canvases: {
          columns: [
            {
              name: "chatId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "chats",
            },
          ],
          policies: {
            select: {
              using: {
                type: "Inherits",
                operation: "Select",
                via_column: "chatId",
              },
            },
          },
        },
        strokes: {
          columns: [
            {
              name: "canvasId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "canvases",
            },
          ],
          policies: {
            select: {
              using: {
                type: "Inherits",
                operation: "Select",
                via_column: "canvasId",
              },
            },
          },
        },
      }),
      "strokes",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "canvasId" }],
      joinCount: 0,
    });
  });

  it("serializes direct Inherits without a parent operation policy as a native inherits policy atom", () => {
    const policy = readSchemaPolicyInherits(
      encodeSchema({
        messages: {
          columns: [{ name: "body", column_type: { type: "Text" }, nullable: false }],
        },
        reactions: {
          columns: [
            {
              name: "message_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "messages",
            },
          ],
          policies: {
            delete: {
              using: {
                type: "Inherits",
                operation: "Delete",
                via_column: "message_id",
              },
            },
          },
        },
      }),
      "reactions",
      "delete",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "message_id" }],
      joinCount: 0,
    });
  });
});

describe("NativeRuntimeAdapter TS adapter perf canary", () => {
  it.skipIf(process.env.JAZZ_TS_ADAPTER_PERF !== "1")(
    "measures reset delivery for one large subscription and many small subscriptions",
    () => {
      const largeRows = Array.from({ length: 24_000 }, (_, index) => ({
        table: "todos",
        rowId: indexedUuidBytes(index + 1),
        title: `large-${index}`,
      }));
      const smallChunks = Array.from({ length: 95 }, (_, subscriptionIndex) =>
        Array.from({ length: 7 }, (_, rowIndex) => ({
          table: "todos",
          rowId: indexedUuidBytes(100_000 + subscriptionIndex * 100 + rowIndex),
          title: `small-${subscriptionIndex}-${rowIndex}`,
        })),
      );

      const measurements: Array<{ label: string; rows: number; ms: number }> = [];
      const runSubscription = (label: string, rows: EncodedTestRow[]) => {
        const chunk = {
          type: "delta",
          reset: true,
          settled: true,
          delta: encodeSubscriptionDelta({ added: rows, updated: [], removed: [] }),
        };
        const runtime = runtimeWithNativeSubscriptionChunk(chunk);
        let callbackCount = 0;
        let addedCount = 0;
        const handle = runtime.createSubscription(
          JSON.stringify({ table: "todos" }),
          null,
          null,
          null,
        );
        const started = performance.now();
        runtime.executeSubscription(handle, (delta: NativeRowDelta) => {
          subscriptionFrameBuffersForTest(delta);
          addedCount += delta.addedCount;
          callbackCount += 1;
        });
        const ms = performance.now() - started;
        expect(callbackCount).toBe(1);
        expect(addedCount).toBe(rows.length);
        measurements.push({ label, rows: rows.length, ms });
        runtime.close();
      };

      runSubscription("large-reset", largeRows);
      for (let index = 0; index < smallChunks.length; index += 1) {
        runSubscription(`small-reset-${index}`, smallChunks[index]!);
      }

      const smallMs = measurements.slice(1).reduce((sum, measurement) => sum + measurement.ms, 0);
      const smallMedian =
        measurements
          .slice(1)
          .map((measurement) => measurement.ms)
          .sort((left, right) => left - right)[Math.floor(smallChunks.length / 2)] ?? 0;
      console.info(
        JSON.stringify({
          largeMs: measurements[0]!.ms,
          smallTotalMs: smallMs,
          smallMedianMs: smallMedian,
          smallCount: smallChunks.length,
        }),
      );
    },
  );

  it.skipIf(process.env.JAZZ_TS_ADAPTER_PERF !== "1" || !hasJazzWasmBuild())(
    "measures mergeable commit slope with and without one maintained subscription",
    { timeout: 900_000 },
    async () => {
      const batchSize = 500;
      const totalRows = 5_000;

      async function run(label: string, withSubscription: boolean) {
        const runtime = (await createWasmRuntime(testSchema, {
          appId: `adapter-perf-${label}`,
          userBranch: String(Date.now()),
        })) as NativeRuntimeAdapter;
        let callbackCount = 0;
        if (withSubscription) {
          const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null);
          runtime.executeSubscription(handle, () => {
            callbackCount += 1;
          });
          await flushAdapterMicrotasks();
        }

        const batches: Array<{ rows: number; commitMs: number }> = [];
        for (let start = 0; start < totalRows; start += batchSize) {
          const tx = runtime.beginTransaction("mergeable");
          for (let offset = 0; offset < batchSize; offset += 1) {
            const index = start + offset + 1;
            runtime.insert(
              "todos",
              { title: { type: "Text", value: `row-${index}` } },
              JSON.stringify({ batch_id: tx }),
              formatUuidForTest(indexedUuidBytes(index)),
            );
          }
          const beforeCommit = performance.now();
          runtime.commitTransaction(tx);
          await flushAdapterMicrotasks();
          const measurement = {
            rows: start + batchSize,
            commitMs: performance.now() - beforeCommit,
          };
          batches.push(measurement);
          console.info(JSON.stringify({ commitSlopeBatch: { label, ...measurement } }));
        }
        runtime.close();
        return { label, withSubscription, callbackCount, batches };
      }

      console.info(
        JSON.stringify({
          commitSlope: [await run("no-subscription", false), await run("one-subscription", true)],
        }),
      );
    },
  );
});

const testSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;

function emptyNativeRuntime(): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => new Uint8Array([0]),
          attachQuery: () => ({}),
          queryAttachmentIsCovered: () => true,
          detachQuery: () => undefined,
          prepareQuery: () => ({}),
          subscribe: () => new ReadableStream(),
          subscribeForIdentity: () => new ReadableStream(),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    new Uint8Array(16),
    1,
    true,
  );
}

function runtimeWithNativeSubscriptionChunk(
  chunk: unknown,
  schema: WasmSchema = testSchema,
): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => new Uint8Array([0]),
          prepareQuery: () => ({}),
          subscribe: () => ({
            readAll: () => [chunk],
            close: () => true,
          }),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    schema,
    new Uint8Array(16),
    new Uint8Array(16),
    1,
    true,
  );
}

function indexedUuidBytes(index: number): Uint8Array {
  const bytes = new Uint8Array(16);
  new DataView(bytes.buffer).setUint32(12, index, false);
  return bytes;
}

function subscriptionFrameBuffersForTest(delta: NativeRowDelta): ArrayBuffer[] {
  return [
    transferableBufferForTest(delta.added),
    transferableBufferForTest(delta.removed),
    transferableBufferForTest(delta.updated),
  ];
}

async function flushAdapterMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function transferableBufferForTest(bytes: Uint8Array): ArrayBuffer {
  if (bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength) {
    return bytes.buffer as ArrayBuffer;
  }
  return bytes.slice().buffer;
}

function readPreparedComparison(query: Uint8Array): {
  table: string;
  predicateTag: number;
  column: string;
  literalTag: number;
  value: string;
  limit: number | undefined;
} {
  const reader = new PostcardReader(query);
  const table = reader.string();
  const predicateCount = reader.u64();
  expect(predicateCount).toBe(1);
  const predicateTag = reader.u64();
  const leftOperandTag = reader.u64();
  expect(leftOperandTag).toBe(0);
  const column = reader.string();
  const rightOperandTag = reader.u64();
  expect(rightOperandTag).toBe(3);
  const literalTag = reader.u64();
  const value = reader.string();
  const tail = readPreparedQueryTail(reader);
  const limit = tail.limit;
  return { table, predicateTag, column, literalTag, value, limit };
}

function readPreparedUuidComparison(query: Uint8Array): {
  table: string;
  predicateTag: number;
  column: string;
  literalTag: number;
  value: string;
  limit: number | undefined;
} {
  const reader = new PostcardReader(query);
  const table = reader.string();
  const predicateCount = reader.u64();
  expect(predicateCount).toBe(1);
  const predicateTag = reader.u64();
  const leftOperandTag = reader.u64();
  expect(leftOperandTag).toBe(0);
  const column = reader.string();
  const rightOperandTag = reader.u64();
  expect(rightOperandTag).toBe(3);
  const literalTag = reader.u64();
  const value = formatUuidForTest(reader.bytes());
  const tail = readPreparedQueryTail(reader);
  const limit = tail.limit;
  return { table, predicateTag, column, literalTag, value, limit };
}

function readPreparedUuidIn(query: Uint8Array): {
  table: string;
  column: string;
  values: string[];
} {
  const reader = new PostcardReader(query);
  const table = reader.string();
  const predicateCount = reader.u64();
  expect(predicateCount).toBe(1);
  expect(reader.u64()).toBe(5);
  expect(reader.u64()).toBe(0);
  const column = reader.string();
  const values = reader.readVec((valueReader) => {
    expect(valueReader.u64()).toBe(3);
    expect(valueReader.u64()).toBe(8);
    return formatUuidForTest(valueReader.bytes());
  });
  return { table, column, values };
}

function readPreparedInLiterals(
  query: Uint8Array,
): Array<{ column: string; literals: Array<{ tag: number; value: number | bigint }> }> {
  const reader = new PostcardReader(query);
  reader.string();
  return reader.readVec((predicateReader) => {
    expect(predicateReader.u64()).toBe(5);
    expect(predicateReader.u64()).toBe(0);
    const column = predicateReader.string();
    const literals = predicateReader.readVec((valueReader) => {
      expect(valueReader.u64()).toBe(3);
      return readPreparedNumericLiteral(valueReader);
    });
    return { column, literals };
  });
}

function readPreparedComparisonLiterals(query: Uint8Array): Array<{
  predicateTag: number;
  column: string;
  literal: { tag: number; value: number | bigint };
}> {
  const reader = new PostcardReader(query);
  reader.string();
  return reader.readVec((predicateReader) => {
    const predicateTag = predicateReader.u64();
    expect(predicateReader.u64()).toBe(0);
    const column = predicateReader.string();
    expect(predicateReader.u64()).toBe(3);
    return { predicateTag, column, literal: readPreparedNumericLiteral(predicateReader) };
  });
}

function readPreparedNumericLiteral(reader: PostcardReader): {
  tag: number;
  value: number | bigint;
} {
  const tag = reader.u64();
  switch (tag) {
    case 2:
    case 3:
      return { tag, value: reader.u64() };
    case 4:
      return { tag, value: reader.f64Le() };
    case 13:
      return { tag, value: reader.i64() };
    default:
      throw new Error(`expected numeric prepared literal tag, got ${tag}`);
  }
}

function encodeSignedI32ForTest(value: number): number {
  return (value ^ 0x80000000) >>> 0;
}

function readPreparedLimit(query: Uint8Array): number | undefined {
  const reader = new PostcardReader(query);
  reader.string();
  reader.readVec(() => {
    skipPreparedPredicate(reader);
  });
  return readPreparedQueryTail(reader).limit;
}

function skipPreparedPredicate(reader: PostcardReader): void {
  const predicateTag = reader.u64();
  if (predicateTag === 5) {
    skipPreparedOperand(reader);
    reader.readVec(() => {
      skipPreparedOperand(reader);
    });
    return;
  }
  skipPreparedOperand(reader);
  skipPreparedOperand(reader);
}

function skipPreparedOperand(reader: PostcardReader): void {
  const operandTag = reader.u64();
  if (operandTag === 0) {
    reader.string();
    return;
  }
  expect(operandTag).toBe(3);
  skipPreparedLiteral(reader);
}

function skipPreparedLiteral(reader: PostcardReader): void {
  const literalTag = reader.u64();
  switch (literalTag) {
    case 2:
    case 3:
      reader.u64();
      return;
    case 4:
      reader.f64Le();
      return;
    case 13:
      reader.i64();
      return;
    case 5:
      reader.bool();
      return;
    case 6:
      reader.string();
      return;
    case 7:
    case 8:
      reader.bytes();
      return;
    case 11:
      reader.readVec(() => {
        skipPreparedLiteral(reader);
      });
      return;
    case 12:
      reader.option(() => {
        skipPreparedLiteral(reader);
      });
      return;
    default:
      throw new Error(`unsupported prepared literal tag ${literalTag}`);
  }
}

function readPreparedQueryTail(
  reader: PostcardReader,
  opts: { prefixAlreadySkipped?: boolean } = {},
): {
  select: string[] | undefined;
  orderBy: Array<{ column: string; directionTag: number }>;
  limit: number | undefined;
  offset: number;
} {
  if (!opts.prefixAlreadySkipped) {
    reader.readVec(() => undefined); // joins
    reader.readVec(() => undefined); // policy_branches
    reader.readVec(() => undefined); // reachable
    reader.readVec(() => undefined); // inherits
    reader.readVec(() => undefined); // includes
    reader.readVec(() => undefined); // array_subqueries
  }
  const select = reader.option((selectReader) => selectReader.readVec(() => selectReader.string()));
  const orderByCount = reader.u64();
  const orderBy = Array.from({ length: orderByCount }, () => ({
    column: reader.string(),
    directionTag: reader.u64(),
  }));
  reader.option(() => undefined); // aggregate
  const limit = reader.option((optionReader) => optionReader.u64());
  const offset = reader.u64();
  return { select, orderBy, limit, offset };
}

function readPreparedSelect(query: Uint8Array): string[] | undefined {
  const reader = new PostcardReader(query);
  reader.string();
  reader.readVec(() => {
    reader.u64();
    reader.u64();
    reader.string();
    reader.u64();
    reader.u64();
    reader.string();
  });
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  return readPreparedQueryTail(reader, { prefixAlreadySkipped: true }).select;
}

function readPreparedQueryShape(query: Uint8Array): {
  table: string;
  predicates: Array<{ column: string; opTag: number; literalTag: number; value: string }>;
  orderBy: Array<{ column: string; directionTag: number }>;
  limit: number | undefined;
  offset: number;
} {
  const reader = new PostcardReader(query);
  const table = reader.string();
  const predicateCount = reader.u64();
  const predicates = Array.from({ length: predicateCount }, () => {
    const opTag = reader.u64();
    expect(reader.u64()).toBe(0);
    const column = reader.string();
    expect(reader.u64()).toBe(3);
    const literalTag = reader.u64();
    const value = reader.string();
    return { column, opTag, literalTag, value };
  });
  const { orderBy, limit, offset } = readPreparedQueryTail(reader);
  return { table, predicates, orderBy, limit, offset };
}

function readPreparedFirstLiteral(query: Uint8Array): {
  column: string;
  opTag: number;
  literalTag: number;
  value: number;
} {
  const reader = new PostcardReader(query);
  reader.string();
  expect(reader.u64()).toBeGreaterThan(0);
  const opTag = reader.u64();
  expect(reader.u64()).toBe(0);
  const column = reader.string();
  expect(reader.u64()).toBe(3);
  const literalTag = reader.u64();
  const value = literalTag === 13 ? Number(reader.i64()) : reader.u64();
  return { column, opTag, literalTag, value };
}

function readSchemaSelectPolicyBranches(
  schemaBytes: Uint8Array,
  tableName: string,
): {
  table: string;
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
  branches: TestPolicyBranch[];
} {
  return readSchemaPolicyBranches(schemaBytes, tableName, "select");
}

function readSchemaPolicyBranches(
  schemaBytes: Uint8Array,
  tableName: string,
  operation: "select" | "insert" | "updateUsing" | "updateCheck" | "delete",
): {
  table: string;
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
  branches: TestPolicyBranch[];
} {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    tableReader.readVec((columnReader) => {
      columnReader.string();
      skipSchemaValueType(columnReader);
      columnReader.option(() => undefined);
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    const policies = {
      select: tableReader.option(readPolicyQueryForTest),
      insert: tableReader.option(readPolicyQueryForTest),
      updateUsing: tableReader.option(readPolicyQueryForTest),
      updateCheck: tableReader.option(readPolicyQueryForTest),
      delete: tableReader.option(readPolicyQueryForTest),
    };
    tableReader.u64();
    const indexCount = tableReader.u64();
    for (let index = 0; index < indexCount; index += 1) {
      tableReader.string();
      tableReader.readVec((indexReader) => indexReader.string());
    }
    return { table, policy: policies[operation] };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);

  const policy = tables.find((table) => table.table === tableName)?.policy;
  expect(policy).toBeDefined();
  return policy!;
}

function readSchemaColumnLargeValues(
  schemaBytes: Uint8Array,
  tableName: string,
): Array<{ name: string; largeValue: "Text" | "Blob" | null }> {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    const columns = tableReader.readVec((columnReader) => {
      const name = columnReader.string();
      skipSchemaValueType(columnReader);
      const largeValue =
        columnReader.option((kindReader) => {
          const tag = kindReader.u64();
          if (tag === 0) return "Text" as const;
          if (tag === 1) return "Blob" as const;
          throw new Error(`unsupported large value kind ${tag}`);
        }) ?? null;
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
      return { name, largeValue };
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.u64();
    const indexCount = tableReader.u64();
    for (let index = 0; index < indexCount; index += 1) {
      tableReader.string();
      tableReader.readVec((indexReader) => indexReader.string());
    }
    return { table, columns };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);
  return tables.find((table) => table.table === tableName)?.columns ?? [];
}

function readSchemaTableMetadata(
  schemaBytes: Uint8Array,
  tableName: string,
): {
  indexedColumns: string[];
  mergeStrategies: Array<{ column: string; strategy: "Lww" | "Counter" }>;
} {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    tableReader.readVec((columnReader) => {
      columnReader.string();
      skipSchemaValueType(columnReader);
      columnReader.option(() => undefined);
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    const indexedColumns = tableReader.readVec((indexReader) => indexReader.string());
    const mergeStrategyCount = tableReader.u64();
    const mergeStrategies: Array<{ column: string; strategy: "Lww" | "Counter" }> = [];
    for (let index = 0; index < mergeStrategyCount; index += 1) {
      const column = tableReader.string();
      const tag = tableReader.u64();
      const strategy = tag === 0 ? "Lww" : tag === 1 ? "Counter" : null;
      if (strategy == null) {
        throw new Error(`unsupported merge strategy tag ${tag}`);
      }
      mergeStrategies.push({ column, strategy });
    }
    return { table, indexedColumns, mergeStrategies };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);
  const table = tables.find((entry) => entry.table === tableName);
  expect(table).toBeDefined();
  return {
    indexedColumns: table!.indexedColumns,
    mergeStrategies: table!.mergeStrategies,
  };
}

function readPolicyQueryForTest(reader: PostcardReader): {
  table: string;
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
  branches: TestPolicyBranch[];
} {
  const table = reader.string();
  const filters = reader.readVec(readPolicyPredicateForTest);
  const joins = reader.readVec(readPolicyJoinForTest);
  const branches = reader.readVec(readPolicyBranchForTest);
  reader.readVec(skipPolicyReachableForTest);
  reader.readVec(readPolicyInheritsForTest);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.option(() => undefined);
  reader.u64();
  return { table, filters, joins, branches };
}

function readSchemaSelectPolicyReachables(
  schemaBytes: Uint8Array,
  tableName: string,
): TestPolicyReachable[] {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    tableReader.readVec((columnReader) => {
      columnReader.string();
      skipSchemaValueType(columnReader);
      columnReader.option(() => undefined);
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    const select = tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.u64();
    const indexCount = tableReader.u64();
    for (let index = 0; index < indexCount; index += 1) {
      tableReader.string();
      tableReader.readVec((indexReader) => indexReader.string());
    }
    return { table, select };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);
  return tables.find((table) => table.table === tableName)?.select?.reachables ?? [];
}

function readSchemaSelectPolicyInherits(
  schemaBytes: Uint8Array,
  tableName: string,
): { inherits: TestPolicyInherits[]; joinCount: number } {
  return readSchemaPolicyInherits(schemaBytes, tableName, "select");
}

function readSchemaPolicyInherits(
  schemaBytes: Uint8Array,
  tableName: string,
  operation: "select" | "insert" | "updateUsing" | "updateCheck" | "delete",
): { inherits: TestPolicyInherits[]; joinCount: number } {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    tableReader.readVec((columnReader) => {
      columnReader.string();
      skipSchemaValueType(columnReader);
      columnReader.option(() => undefined);
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    const policies = {
      select: tableReader.option(readPolicyQueryWithInheritsForTest),
      insert: tableReader.option(readPolicyQueryWithInheritsForTest),
      updateUsing: tableReader.option(readPolicyQueryWithInheritsForTest),
      updateCheck: tableReader.option(readPolicyQueryWithInheritsForTest),
      delete: tableReader.option(readPolicyQueryWithInheritsForTest),
    };
    tableReader.u64();
    const indexCount = tableReader.u64();
    for (let index = 0; index < indexCount; index += 1) {
      tableReader.string();
      tableReader.readVec((indexReader) => indexReader.string());
    }
    return { table, policy: policies[operation] };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);
  return (
    tables.find((table) => table.table === tableName)?.policy ?? { inherits: [], joinCount: 0 }
  );
}

function readPolicyQueryWithReachablesForTest(reader: PostcardReader): {
  reachables: TestPolicyReachable[];
} {
  reader.string();
  reader.readVec(readPolicyPredicateForTest);
  reader.readVec(readPolicyJoinForTest);
  reader.readVec(readPolicyBranchForTest);
  const reachables = reader.readVec(readPolicyReachableForTest);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.option(() => undefined);
  reader.u64();
  return { reachables };
}

function readPolicyQueryWithInheritsForTest(reader: PostcardReader): {
  inherits: TestPolicyInherits[];
  joinCount: number;
} {
  reader.string();
  reader.readVec(readPolicyPredicateForTest);
  const joinCount = reader.readVec(readPolicyJoinForTest).length;
  reader.readVec(readPolicyBranchForTest);
  reader.readVec(skipPolicyReachableForTest);
  const inherits = reader.readVec(readPolicyInheritsForTest);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.option(() => undefined);
  reader.u64();
  return { inherits, joinCount };
}

function readPolicyBranchForTest(reader: PostcardReader): TestPolicyBranch {
  const filters = reader.readVec(readPolicyPredicateForTest);
  const joins = reader.readVec(readPolicyJoinForTest);
  reader.readVec(skipPolicyReachableForTest);
  reader.readVec(readPolicyInheritsForTest);
  return { filters, joins };
}

function readPolicyInheritsForTest(reader: PostcardReader): TestPolicyInherits {
  const parentColumn = reader.string();
  reader.u64();
  return { parentColumn };
}

function readPolicyJoinForTest(reader: PostcardReader): TestPolicyJoin {
  const table = reader.string();
  const onColumn = reader.string();
  const targetTag = reader.u64();
  const sourceColumn = reader.option((sourceReader) => sourceReader.string());
  const sourceLookup = reader.option((lookupReader) => ({
    table: lookupReader.string(),
    rowIdSourceColumn: lookupReader.string(),
    valueColumn: lookupReader.string(),
  }));
  reader.readVec((correlationReader) => {
    correlationReader.string();
    correlationReader.string();
  });
  const filters = reader.readVec(readPolicyPredicateForTest);
  const nestedJoins = reader.readVec(readPolicyJoinForTest);
  return { table, onColumn, targetTag, sourceColumn, sourceLookup, filters, nestedJoins };
}

function skipPolicyReachableForTest(reader: PostcardReader): void {
  readPolicyReachableForTest(reader);
}

function readPolicyReachableForTest(reader: PostcardReader): TestPolicyReachable {
  const accessTable = reader.string();
  const accessRowColumn = reader.string();
  const accessTeamColumn = reader.string();
  const accessTeamTargetTag = reader.u64();
  readPolicyOperandForTest(reader);
  const accessFilters = reader.readVec(readPolicyPredicateForTest);
  const edgeTable = reader.string();
  const edgeMemberColumn = reader.string();
  const edgeParentColumn = reader.string();
  const edgeFilters = reader.readVec(readPolicyPredicateForTest);
  const boundTag = reader.u64();
  const maxDepth = boundTag === 1 ? reader.u64() : 0;
  const seed = reader.option((seedReader) => ({
    table: seedReader.string(),
    userColumn: seedReader.option((userColumnReader) => userColumnReader.string()),
    userClaim: seedReader.option((userClaimReader) => userClaimReader.string()),
    teamColumn: seedReader.string(),
    filters: seedReader.readVec(readPolicyPredicateForTest),
  }));
  return {
    accessTable,
    accessRowColumn,
    accessTeamColumn,
    accessTeamTargetTag,
    accessFilters,
    edgeTable,
    edgeMemberColumn,
    edgeParentColumn,
    edgeFilters,
    maxDepth,
    seed,
  };
}

function readPolicyPredicateForTest(reader: PostcardReader): TestPolicyPredicate {
  const tag = reader.u64();
  if (tag === 0 || tag === 1) {
    return { tag, children: reader.readVec(readPolicyPredicateForTest) };
  }
  if (tag === 2) {
    return { tag, child: readPolicyPredicateForTest(reader) };
  }
  if (tag === 3) {
    expect(reader.u64()).toBe(0);
    const column = reader.string();
    return { tag, column, operand: readPolicyOperandForTest(reader) };
  }
  if (tag === 5) {
    readPolicyOperandForTest(reader);
    reader.readVec(readPolicyOperandForTest);
    return { tag };
  }
  if (tag === 10) {
    readPolicyOperandForTest(reader);
    readPolicyOperandForTest(reader);
    return { tag };
  }
  if (tag === 11) {
    readPolicyOperandForTest(reader);
    return { tag };
  }
  throw new Error(`unsupported policy predicate tag ${tag}`);
}

function readPolicyOperandForTest(reader: PostcardReader): TestPolicyOperand {
  const tag = reader.u64();
  if (tag === 0) return { tag, column: reader.string() };
  if (tag === 2) return { tag, claim: reader.string() };
  if (tag === 3) {
    const literalTag = reader.u64();
    if (literalTag === 2 || literalTag === 3) {
      return { tag, literalTag, value: reader.u64() };
    }
    if (literalTag === 13) {
      return { tag, literalTag, value: reader.i64() };
    }
    if (literalTag === 4) {
      return { tag, literalTag, value: reader.bytes() };
    }
    if (literalTag === 5) {
      return { tag, literalTag, value: reader.bool() };
    }
    if (literalTag === 6) {
      return { tag, literalTag, value: reader.string() };
    }
    if (literalTag === 8) {
      return { tag, literalTag, value: reader.bytes() };
    }
    if (literalTag === 12) {
      return { tag, literalTag, value: reader.option(readPolicyOperandForTest) };
    }
    throw new Error(`unsupported policy literal tag ${literalTag}`);
  }
  throw new Error(`unsupported policy operand tag ${tag}`);
}

function skipSchemaValueType(reader: PostcardReader): void {
  const tag = reader.u64();
  if (tag === 11 || tag === 12) skipSchemaValueType(reader);
}

function skipGrooveValue(reader: PostcardReader): void {
  const tag = reader.u64();
  switch (tag) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 9:
      reader.u64();
      return;
    case 4:
      reader.f64Le();
      return;
    case 5:
      reader.bool();
      return;
    case 6:
      reader.string();
      return;
    case 7:
      reader.bytes();
      return;
    case 8:
      reader.bytes(false);
      return;
    case 10:
    case 11:
      reader.readVec(skipGrooveValue);
      return;
    case 12:
      reader.option(skipGrooveValue);
      return;
    case 13:
      reader.i64();
      return;
    default:
      throw new Error(`unsupported groove value tag ${tag}`);
  }
}

type TestPolicyBranch = {
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
};

type TestPolicyInherits = {
  parentColumn: string;
};

type TestPolicyJoin = {
  table: string;
  onColumn: string;
  targetTag: number;
  sourceColumn: string | undefined;
  sourceLookup: { table: string; rowIdSourceColumn: string; valueColumn: string } | undefined;
  filters: TestPolicyPredicate[];
  nestedJoins: TestPolicyJoin[];
};

type TestPolicyReachable = {
  accessTable: string;
  accessRowColumn: string;
  accessTeamColumn: string;
  accessTeamTargetTag: number;
  accessFilters: TestPolicyPredicate[];
  edgeTable: string;
  edgeMemberColumn: string;
  edgeParentColumn: string;
  edgeFilters: TestPolicyPredicate[];
  maxDepth: number;
  seed:
    | {
        table: string;
        userColumn: string | undefined;
        userClaim: string | undefined;
        teamColumn: string;
        filters: TestPolicyPredicate[];
      }
    | undefined;
};

type TestPolicyPredicate =
  | { tag: number; children: TestPolicyPredicate[] }
  | { tag: number; child: TestPolicyPredicate }
  | { tag: number; column: string; operand: TestPolicyOperand }
  | { tag: number };

type TestPolicyOperand =
  | { tag: number; column: string }
  | { tag: number; claim: string }
  | { tag: number; literalTag: number; value: unknown };

function unsupportedJoinRelationIr(): unknown {
  return {
    Join: {
      left: { TableScan: { table: "todos" } },
      right: { TableScan: { table: "projects" } },
      on: {
        left: { column: "todos.project_id" },
        right: { column: "projects.id" },
      },
    },
  };
}

function unsupportedProjectRelationIr(): unknown {
  return {
    Project: {
      input: { TableScan: { table: "todos" } },
      columns: [{ source: { column: "title" }, alias: "title" }],
    },
  };
}

const binaryLargeValueSchema = {
  binary_large_values: {
    columns: [
      {
        name: "chunk_refs",
        column_type: { type: "Array", element: { type: "Uuid" } },
        nullable: false,
      },
      {
        name: "chunk_sizes",
        column_type: { type: "Array", element: { type: "Double" } },
        nullable: false,
      },
    ],
  },
} satisfies WasmSchema;

class FakeTransport implements Transport {
  closed = false;
  readonly received: Uint8Array[] = [];
  readonly receivedBatches: Uint8Array[][] = [];
  tickCount = 0;

  constructor(private readonly outgoing: Uint8Array[]) {}

  close(): boolean {
    this.closed = true;
    return true;
  }

  recvWireFrames(): unknown[] {
    return this.outgoing.splice(0);
  }

  sendWireFrame(frame: Uint8Array): void {
    this.received.push(frame);
  }

  sendWireFrames(frames: readonly Uint8Array[]): void {
    const batch = [...frames];
    this.receivedBatches.push(batch);
    this.received.push(...batch);
  }

  tick(): number {
    this.tickCount += 1;
    return 0;
  }
}

class FakeWebSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readonly readyState = 1;
  readonly sent: Array<Uint8Array | string> = [];
  private readonly messageListeners: Array<(event: { data: unknown }) => void> = [];
  closed = false;

  constructor(readonly url: string) {}

  send(data: Uint8Array | string): void {
    this.sent.push(data);
  }

  close(): void {
    this.closed = true;
  }

  addEventListener(type: string, listener: (event: { data: unknown }) => void): void {
    if (type === "message") this.messageListeners.push(listener);
  }

  emitMessage(data: Uint8Array): void {
    for (const listener of this.messageListeners) listener({ data });
  }
}

function encodeWireError(code: number, retry: number, message: string): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(2);
  writer.u64(code);
  writer.u64(retry);
  writer.string(message);
  return writer.finish();
}

type EncodedTestRow = {
  table: string;
  rowId: Uint8Array;
  title: string;
  index?: number;
  txTime?: number;
  createdAt?: number;
  updatedAt?: number;
};

function encodeRows(rows: EncodedTestRow[]): Uint8Array {
  const writer = new PostcardWriter();
  writeRowBatches(writer, rows);
  return writer.finish();
}

function encodeRelationSnapshot(
  rows: EncodedTestRow[],
  edges: Array<{
    sourceTable: string;
    sourceRowId: Uint8Array;
    relation: string;
    targetTable: string;
    targetRowId: Uint8Array;
  }>,
  rootCount = rows.length,
): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0);
  writer.u64(rootCount);
  writeRowBatches(writer, rows);
  writer.vec((edge, index) => {
    const source = edges[index]!;
    edge.string(source.sourceTable);
    edge.bytes(source.sourceRowId);
    edge.string(source.relation);
    edge.string(source.targetTable);
    edge.bytes(source.targetRowId);
  }, edges.length);
  return writer.finish();
}

function writeRowBatches(writer: PostcardWriter, rows: EncodedTestRow[]): void {
  const rowsByTable = new Map<string, EncodedTestRow[]>();
  for (const row of rows) {
    const tableRows = rowsByTable.get(row.table) ?? [];
    tableRows.push(row);
    rowsByTable.set(row.table, tableRows);
  }
  writer.vec((batch, batchIndex) => {
    const [table, tableRows] = Array.from(rowsByTable.entries())[batchIndex]!;
    const hasTxTime = tableRows.some((row) => row.txTime !== undefined);
    const hasProvenance = tableRows.some(
      (row) => row.createdAt !== undefined || row.updatedAt !== undefined,
    );
    const descriptor = [
      { name: "title", valueType: { tag: 6 } },
      ...(hasProvenance
        ? [
            { name: "$createdAt", valueType: { tag: 3 } },
            { name: "$updatedAt", valueType: { tag: 3 } },
          ]
        : []),
      ...(hasTxTime ? [{ name: "tx_time", valueType: { tag: 3 } }] : []),
    ];
    batch.string(table);
    writeDescriptor(batch, descriptor);
    batch.vec((row, index) => {
      const source = tableRows[index]!;
      row.bytes(source.rowId);
      row.u64(source.index ?? index);
      row.bool(false);
      const values: Uint8Array[] = [new TextEncoder().encode(source.title)];
      if (hasProvenance) {
        values.push(u64Bytes(source.createdAt ?? 0));
        values.push(u64Bytes(source.updatedAt ?? 0));
      }
      if (hasTxTime) {
        values.push(txTimeBytes(source.txTime ?? 0));
      }
      row.bytes(createRecord(descriptor, values));
    }, tableRows.length);
  }, rowsByTable.size);
}

function encodeSubscriptionDelta(delta: {
  added: EncodedTestRow[];
  updated: EncodedTestRow[];
  removed: Array<{ table: string; rowId: Uint8Array; index?: number }>;
}): Uint8Array {
  const writer = new PostcardWriter();
  writeRowBatches(writer, delta.added);
  writeRowBatches(writer, delta.updated);
  writer.vec((removed, index) => {
    const source = delta.removed[index]!;
    removed.string(source.table);
    removed.bytes(source.rowId);
    removed.u64(source.index ?? index);
  }, delta.removed.length);
  return writer.finish();
}

function encodeRelationSubscriptionDelta(delta: {
  baseCursor?: number;
  cursor: number;
  added: EncodedTestRow[];
  updated: EncodedTestRow[];
  removed: Array<{ table: string; rowId: Uint8Array; index?: number }>;
  addedEdges: NativeRelationSubscriptionEdge[];
  removedEdges: NativeRelationSubscriptionEdge[];
}): Uint8Array {
  const writer = new PostcardWriter();
  if (delta.baseCursor === undefined) {
    writer.none();
  } else {
    writer.some((value) => value.u64(delta.baseCursor!));
  }
  writer.u64(delta.cursor);
  writeRowBatches(writer, delta.added);
  writeRowBatches(writer, delta.updated);
  writer.vec((removed, index) => {
    const source = delta.removed[index]!;
    removed.string(source.table);
    removed.bytes(source.rowId);
    removed.u64(source.index ?? index);
  }, delta.removed.length);
  writer.vec(
    (edge, index) => writeRelationEdge(edge, delta.addedEdges[index]!),
    delta.addedEdges.length,
  );
  writer.vec(
    (edge, index) => writeRelationEdge(edge, delta.removedEdges[index]!),
    delta.removedEdges.length,
  );
  return writer.finish();
}

function encodeUserWrappedSubscriptionDelta(row: {
  table: string;
  rowId: Uint8Array;
  title: string;
  note: string;
}): Uint8Array {
  const descriptor = [
    { name: "row_uuid", valueType: { tag: 8 } },
    { name: "user_title", valueType: { tag: 12, inner: { tag: 6 } } },
    { name: "user_note", valueType: { tag: 12, inner: { tag: 12, inner: { tag: 6 } } } },
    { name: "$createdAt", valueType: { tag: 3 } },
  ];
  const delta = new PostcardWriter();
  delta.vec((batch) => {
    batch.string(row.table);
    writeDescriptor(batch, descriptor);
    batch.vec((encodedRow) => {
      encodedRow.bytes(row.rowId);
      encodedRow.u64(0);
      encodedRow.bool(false);
      encodedRow.bytes(
        createRecord(descriptor, [
          row.rowId,
          presentBytes(new TextEncoder().encode(row.title)),
          presentBytes(presentBytes(new TextEncoder().encode(row.note))),
          u64Bytes(123),
        ]),
      );
    }, 1);
  }, 1);
  delta.vec(() => undefined, 0);
  delta.vec(() => undefined, 0);
  return delta.finish();
}

function presentBytes(bytes: Uint8Array): Uint8Array {
  const output = new Uint8Array(bytes.length + 1);
  output[0] = 1;
  output.set(bytes, 1);
  return output;
}

function writeRelationEdge(writer: PostcardWriter, edge: NativeRelationSubscriptionEdge): void {
  writer.string(edge.sourceTable);
  writer.bytes(edge.sourceRowId);
  writer.string(edge.relation);
  writer.string(edge.targetTable);
  writer.bytes(edge.targetRowId);
}

function encodeBinaryLargeValueRows(): Uint8Array {
  const descriptor = [
    { name: "chunk_refs", valueType: { tag: 11, inner: { tag: 8 } } },
    { name: "chunk_sizes", valueType: { tag: 11, inner: { tag: 4 } } },
  ];
  const writer = new PostcardWriter();
  writer.vec((batch) => {
    batch.string("binary_large_values");
    writeDescriptor(batch, descriptor);
    batch.vec((row) => {
      row.bytes(uuidBytes("00000000-0000-0000-0000-000000000010"));
      row.u64(0);
      row.bool(false);
      row.bytes(
        createRecord(descriptor, [
          concatBytes([
            uuidBytes("00000000-0000-0000-0000-000000000001"),
            uuidBytes("00000000-0000-0000-0000-000000000002"),
          ]),
          concatBytes([doubleBytes(65536), doubleBytes(1234)]),
        ]),
      );
    }, 1);
  }, 1);
  return writer.finish();
}

function fakeDb<T extends object>(
  db: T,
): T & { setTickScheduler(callback: (urgency: "immediate" | "deferred") => void): void } {
  return {
    setTickScheduler: () => undefined,
    ...db,
  };
}

function fakeTx(overrides: Partial<TxForTest> = {}): TxForTest {
  return {
    commit: () => fakeWrite(),
    rollback: () => undefined,
    insertWithIdEncoded: () => undefined,
    restoreEncoded: () => undefined,
    updateEncoded: () => undefined,
    upsertEncoded: () => undefined,
    delete: () => undefined,
    ...overrides,
  };
}

function fakeWrite() {
  return {
    payload: new Uint8Array(0),
    wait: () => undefined,
    writeState: () => ({}),
    nextWriteStateChange: async () => undefined,
  };
}

type TxForTest = {
  commit(): ReturnType<typeof fakeWrite>;
  rollback(): void;
  insertWithIdEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void;
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void;
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    updatedAtMs?: number | null,
  ): void;
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void;
  delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): void;
};

function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function formatUuidForTest(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function sameBytesForTest(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  return left.every((byte, index) => byte === right[index]);
}

function doubleBytes(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, true);
  return bytes;
}

function txTimeBytes(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value) << 16n, true);
  return bytes;
}

function u64Bytes(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
  return bytes;
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
