import { afterEach, describe, expect, it } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import { createRecord, PostcardReader, PostcardWriter, writeDescriptor } from "./direct-codec.js";
import {
  decodeDirectWebSocketFrameBatch,
  encodeDirectWebSocketPrelude,
  encodeDirectWebSocketFrameBatch,
  isDirectWireHello,
} from "./direct-websocket.js";
import { DirectCoreRuntime, type DirectTransport } from "./runtime.js";

const previousWebSocket = globalThis.WebSocket;

describe("DirectCoreRuntime server transport", () => {
  afterEach(() => {
    globalThis.WebSocket = previousWebSocket;
  });

  it("connects the direct upstream transport to the scoped websocket endpoint", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([Uint8Array.from([1, 2, 3])]);
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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
      encodeDirectWebSocketPrelude(
        "{}",
        Uint8Array.from([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
      ),
    );
    const helloBatch = decodeDirectWebSocketFrameBatch(sockets[0]!.sent[1]!);
    expect(helloBatch).toHaveLength(1);
    expect(isDirectWireHello(helloBatch[0]!)).toBe(true);
    expect(decodeDirectWebSocketFrameBatch(sockets[0]!.sent[2]!)).toEqual([
      Uint8Array.from([1, 2, 3]),
    ]);
    expect(transport.closed).toBe(false);

    runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    await Promise.resolve();
    await Promise.resolve();

    expect(sockets).toHaveLength(2);
    expect(sockets[0]!.closed).toBe(true);
    expect(JSON.parse(new TextDecoder().decode(sockets[1]!.sent[0]))).toEqual({
      peer_identity: "01010101010101010101010101010101",
      auth: { jwt_token: "fresh.jwt" },
    });

    runtime.disconnect();

    expect(sockets[1]!.closed).toBe(true);
  });

  it("reports direct websocket auth failures through the auth failure callback", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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
      encodeDirectWebSocketFrameBatch([encodeDirectWireError(3, 1, "token expired")]),
    );
    await Promise.resolve();

    expect(authFailures).toEqual(["expired"]);
    expect(transport.received).toEqual([]);
  });

  it("does not report non-auth direct websocket errors as auth failures", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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
      encodeDirectWebSocketFrameBatch([encodeDirectWireError(5, 3, "conflicting commit unit")]),
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
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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

  it("decodes fixed-width array columns from direct row batches", async () => {
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
          all: () => encodeFileRows(),
          prepareQuery: () => ({}),
          tick: () => undefined,
        }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      fileSchema,
      new Uint8Array(16),
      new Uint8Array(16),
      1,
      true,
    );

    await expect(runtime.query(JSON.stringify({ table: "files" }))).resolves.toEqual([
      {
        table: "files",
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

  it("lowers scalar comparison relation IR into the prepared direct query", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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

  it("keeps simple equality relation queries supported", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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

  it("rejects unsupported relation query shapes before preparing or reading", async () => {
    const calls: string[] = [];
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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
    ).rejects.toThrow("refusing to run an overbroad table query");
    expect(calls).toEqual([]);
  });

  it("rejects unsupported subscription relation shapes before subscribing", () => {
    const calls: string[] = [];
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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
    ).toThrow("refusing to run an overbroad table query");
    expect(calls).toEqual([]);
  });

  it("passes supported read tiers through and fails fast for unsupported read options", async () => {
    const runtime = directRuntimeWithEmptyDb();

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

  it("passes include_deleted query intent through direct read options", async () => {
    const readOptions: unknown[] = [];
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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

  it("passes supported subscription read tiers through", () => {
    const runtime = directRuntimeWithEmptyDb();

    expect(() =>
      runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "edge"),
    ).not.toThrow();
    expect(() =>
      runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "planetary"),
    ).toThrow("unsupported read tier");
  });

  it("accepts well-formed subscription sessions and rejects malformed sessions", () => {
    const runtime = directRuntimeWithEmptyDb();

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
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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
    const snapshots: string[][] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.executeSubscription(
      handle,
      (delta: Array<{ row: { values: Array<{ value: string }> } }>) => {
        snapshots.push(delta.map((entry) => entry.row.values[0]!.value));
      },
    );

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

    expect(snapshots).toEqual([
      ["first", "second"],
      ["second updated", "third"],
    ]);
  });

  it("fails fast instead of dropping unsupported id comparisons", async () => {
    const runtime = directRuntimeWithEmptyDb();

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
    ).rejects.toThrow("does not support 'Gt' comparisons on id yet");
  });

  it("does not push limits below post-filtered id predicates", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new DirectCoreRuntime(
      {
        openMemory: () => ({
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

    expect(readPreparedLimit(preparedBytes!)).toBeUndefined();
  });
});

const testSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;

function directRuntimeWithEmptyDb(): DirectCoreRuntime {
  return new DirectCoreRuntime(
    {
      openMemory: () => ({
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
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  const limit = reader.option((optionReader) => optionReader.u64());
  return { table, predicateTag, column, literalTag, value, limit };
}

function readPreparedLimit(query: Uint8Array): number | undefined {
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
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  return reader.option((optionReader) => optionReader.u64());
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

const fileSchema = {
  files: {
    columns: [
      {
        name: "partIds",
        column_type: { type: "Array", element: { type: "Uuid" } },
        nullable: false,
      },
      {
        name: "partSizes",
        column_type: { type: "Array", element: { type: "Double" } },
        nullable: false,
      },
    ],
  },
} satisfies WasmSchema;

class FakeTransport implements DirectTransport {
  closed = false;
  readonly received: Uint8Array[] = [];

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
    return 0;
  }
}

class FakeWebSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readonly readyState = 1;
  readonly sent: Uint8Array[] = [];
  private readonly messageListeners: Array<(event: { data: unknown }) => void> = [];
  closed = false;

  constructor(readonly url: string) {}

  send(data: Uint8Array): void {
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

function encodeDirectWireError(code: number, retry: number, message: string): Uint8Array {
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

function encodeFileRows(): Uint8Array {
  const descriptor = [
    { name: "partIds", valueType: { tag: 11, inner: { tag: 8 } } },
    { name: "partSizes", valueType: { tag: 11, inner: { tag: 4 } } },
  ];
  const writer = new PostcardWriter();
  writer.vec((batch) => {
    batch.string("files");
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
