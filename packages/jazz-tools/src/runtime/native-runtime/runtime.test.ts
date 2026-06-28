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

  it("uses the binding scheduler without manually ticking the db during server pumps", async () => {
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
    expect(dbTicks).toBe(0);
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

    await runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");

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

    await runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));

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
      rows: encodeRows([
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

  it("rejects Join relation IR before preparing or reading", async () => {
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
    ).rejects.toThrow(
      'Relation IR operator "Join" requires a relation-tree lowerer or native relation query API',
    );
    expect(calls).toEqual([]);
  });

  it("rejects Project relation IR while preparing the original subscription query", () => {
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
        JSON.stringify({ table: "todos", relation_ir: unsupportedProjectRelationIr() }),
      ),
    ).toThrow(
      'Relation IR operator "Project" requires a relation-tree lowerer or native relation query API',
    );
    expect(calls).toEqual([]);
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

  it("uses native subscription chunks for array subquery subscriptions", async () => {
    const calls: string[] = [];
    let controller: ReadableStreamDefaultController<unknown> | undefined;
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
    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        array_subqueries: [
          {
            column_name: "children",
            table: "todos",
            inner_column: "parent_id",
            outer_column: "id",
          },
        ],
      }),
    );
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue({
      type: "snapshot",
      rows: encodeRows([
        {
          table: "todos",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
          title: "native",
        },
      ]),
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
    ).toThrow(
      'Relation IR operator "Gather" requires a relation-tree lowerer or native relation query API',
    );
    expect(calls).toEqual([]);
  });

  it("passes supported read tiers and propagation through native read options", async () => {
    const readOptions: unknown[] = [];
    const propagated: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new Uint8Array([0]);
            },
            propagateQuery: (_query: unknown, opts: unknown) => {
              propagated.push(opts);
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
        null,
        "edge",
        JSON.stringify({ propagation: "local-only" }),
      ),
    ).resolves.toEqual([]);

    expect(readOptions).toEqual([{ tier: "edge", propagation: "local_only" }]);
    expect(propagated).toEqual([]);
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
            propagateQuery: () => undefined,
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
      let allCalls = 0;
      const runtime = new NativeRuntimeAdapter(
        {
          openMemory: () =>
            fakeDb({
              all: () => {
                allCalls += 1;
                return new Uint8Array([0]);
              },
              connectUpstream: () => transport,
              prepareQuery: () => ({}),
              propagateQuery: () => undefined,
              queryIsCovered: () => covered,
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
      expect(allCalls).toBe(0);

      covered = true;
      await vi.advanceTimersByTimeAsync(10);

      await expect(query).resolves.toEqual([]);
      expect(allCalls).toBe(1);
    } finally {
      vi.useRealTimers();
    }
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
    const propagated: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            prepareQuery: () => ({}),
            subscribe: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new ReadableStream();
            },
            propagateQuery: (_query: unknown, opts: unknown) => {
              propagated.push(opts);
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
    expect(propagated).toEqual([]);
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
      rows: encodeRows([
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
      ]),
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
      rows: encodeRows([
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
      ]),
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
          propagateQuery: () => undefined,
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
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option((selectReader) => selectReader.readVec(() => selectReader.string()));
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  const limit = reader.option((optionReader) => optionReader.u64());
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
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option((selectReader) => selectReader.readVec(() => selectReader.string()));
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  const limit = reader.option((optionReader) => optionReader.u64());
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
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  return reader.option((optionReader) => optionReader.u64());
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
  return reader.option((selectReader) => selectReader.readVec(() => selectReader.string()));
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
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option((selectReader) => selectReader.readVec(() => selectReader.string()));
  const orderByCount = reader.u64();
  const orderBy = Array.from({ length: orderByCount }, () => ({
    column: reader.string(),
    directionTag: reader.u64(),
  }));
  reader.option(() => undefined);
  const limit = reader.option((optionReader) => optionReader.u64());
  const offset = reader.u64();
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
