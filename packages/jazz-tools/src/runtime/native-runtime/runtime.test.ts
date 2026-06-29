import { afterEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import { createRecord, PostcardReader, PostcardWriter, writeDescriptor } from "./native-codec.js";
import {
  decodeWebSocketFrameBatch,
  encodeWebSocketPrelude,
  encodeWebSocketFrameBatch,
  isWireHello,
} from "./websocket.js";
import { NativeRuntimeAdapter, type Transport } from "./native-runtime-adapter.js";
import { encodeSchema } from "./schema-codec.js";

const previousWebSocket = globalThis.WebSocket;

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

    expect(transport.tickCount).toBeGreaterThan(0);
    expect(dbTicks).toBe(0);

    schedulerCallback?.("immediate");
    await Promise.resolve();

    expect(transport.tickCount).toBeGreaterThan(1);
    expect(dbTicks).toBe(1);
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
    expect(deltas).toEqual([
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

    expect(deltas).toEqual([
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
      ),
    });
    await Promise.resolve();

    expect(calls).toEqual(["prepareQuery", "subscribe"]);
    expect(deltas).toEqual([
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
    expect(deltas).toEqual([
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
              max_depth: 3,
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
          },
        ],
        updated: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
            title: "second updated",
          },
        ],
        removed: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
          },
        ],
      }),
    });
    await Promise.resolve();

    expect(deltas).toEqual([
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

    expect(deltas[0]).toHaveLength(2);
    expect(readPreparedUuidComparison(preparedBytes!)).toMatchObject({
      table: "todos",
      predicateTag: 3,
      column: "id",
      literalTag: 8,
      value: "00000000-0000-0000-0000-000000000001",
    });
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

  it("serializes direct Inherits delete through the parent delete policy", () => {
    const policy = readSchemaPolicyBranches(
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
      table: "reactions",
      filters: [],
      joins: [
        {
          table: "messages",
          onColumn: "id",
          targetTag: 1,
          sourceColumn: "message_id",
          sourceLookup: undefined,
          filters: [
            {
              tag: 3,
              column: "room_id",
              operand: { tag: 2, claim: "roomId" },
            },
          ],
          nestedJoins: [],
        },
      ],
      branches: [],
    });
  });

  it("serializes direct Inherits without a parent operation policy as fail-closed", () => {
    const policy = readSchemaPolicyBranches(
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
      table: "reactions",
      filters: [{ tag: 1, children: [] }],
      joins: [],
      branches: [],
    });
  });
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
      reader.u64();
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
  const value = reader.u64();
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
          if (tag === 0) return "Text";
          if (tag === 1) return "Blob";
          throw new Error(`unsupported large value kind ${tag}`);
        }) ?? null;
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
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.option(() => undefined);
  reader.u64();
  return { table, filters, joins, branches };
}

function readPolicyBranchForTest(reader: PostcardReader): TestPolicyBranch {
  const filters = reader.readVec(readPolicyPredicateForTest);
  const joins = reader.readVec(readPolicyJoinForTest);
  reader.readVec(skipPolicyReachableForTest);
  return { filters, joins };
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
  reader.string();
  reader.string();
  reader.string();
  reader.u64();
  readPolicyOperandForTest(reader);
  reader.readVec(readPolicyPredicateForTest);
  reader.string();
  reader.string();
  reader.string();
  reader.readVec(readPolicyPredicateForTest);
  reader.u64();
  reader.option((seedReader) => {
    seedReader.string();
    seedReader.string();
    seedReader.readVec(readPolicyPredicateForTest);
  });
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
  throw new Error(`unsupported policy predicate tag ${tag}`);
}

function readPolicyOperandForTest(reader: PostcardReader): TestPolicyOperand {
  const tag = reader.u64();
  if (tag === 0) return { tag, column: reader.string() };
  if (tag === 2) return { tag, claim: reader.string() };
  if (tag === 3) {
    const literalTag = reader.u64();
    expect(literalTag).toBe(6);
    return { tag, literalTag, value: reader.string() };
  }
  throw new Error(`unsupported policy operand tag ${tag}`);
}

function skipSchemaValueType(reader: PostcardReader): void {
  const tag = reader.u64();
  if (tag === 11 || tag === 12) skipSchemaValueType(reader);
}

type TestPolicyBranch = {
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
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

type TestPolicyPredicate =
  | { tag: number; children: TestPolicyPredicate[] }
  | { tag: number; child: TestPolicyPredicate }
  | { tag: number; column: string; operand: TestPolicyOperand };

type TestPolicyOperand =
  | { tag: number; column: string }
  | { tag: number; claim: string }
  | { tag: number; literalTag: number; value: string };

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

function encodeRows(rows: Array<{ table: string; rowId: Uint8Array; title: string }>): Uint8Array {
  const writer = new PostcardWriter();
  writeRowBatches(writer, rows);
  return writer.finish();
}

function encodeRelationSnapshot(
  rows: Array<{ table: string; rowId: Uint8Array; title: string }>,
  edges: Array<{
    sourceTable: string;
    sourceRowId: Uint8Array;
    relation: string;
    targetTable: string;
    targetRowId: Uint8Array;
  }>,
): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0);
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

function writeRowBatches(
  writer: PostcardWriter,
  rows: Array<{ table: string; rowId: Uint8Array; title: string }>,
): void {
  const rowsByTable = new Map<string, Array<{ rowId: Uint8Array; title: string }>>();
  for (const row of rows) {
    const tableRows = rowsByTable.get(row.table) ?? [];
    tableRows.push(row);
    rowsByTable.set(row.table, tableRows);
  }
  const descriptor = [{ name: "title", valueType: { tag: 6 } }];
  writer.vec((batch, batchIndex) => {
    const [table, tableRows] = Array.from(rowsByTable.entries())[batchIndex]!;
    batch.string(table);
    writeDescriptor(batch, descriptor);
    batch.vec((row, index) => {
      const source = tableRows[index]!;
      row.bytes(source.rowId);
      row.bool(false);
      row.bytes(createRecord(descriptor, [new TextEncoder().encode(source.title)]));
    }, tableRows.length);
  }, rowsByTable.size);
}

function encodeSubscriptionDelta(delta: {
  added: Array<{ table: string; rowId: Uint8Array; title: string }>;
  updated: Array<{ table: string; rowId: Uint8Array; title: string }>;
  removed: Array<{ table: string; rowId: Uint8Array }>;
}): Uint8Array {
  const writer = new PostcardWriter();
  writeRowBatches(writer, delta.added);
  writeRowBatches(writer, delta.updated);
  writer.vec((removed, index) => {
    const source = delta.removed[index]!;
    removed.string(source.table);
    removed.bytes(source.rowId);
  }, delta.removed.length);
  return writer.finish();
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
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): void;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  delete(table: string, rowId: Uint8Array): void;
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

function doubleBytes(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, true);
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
