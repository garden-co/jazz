import { afterEach, describe, expect, it } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  CLIENT_WIRE_FEATURES,
  decodeWebSocketFrameBatch,
  encodeWireClientHello,
  encodeWebSocketPrelude,
  encodeWebSocketFrameBatch,
  isWireHello,
  WIRE_PROTOCOL_VERSION,
} from "./websocket.js";
import { NativeRuntimeAdapter, type Transport } from "./native-runtime-adapter.js";

const previousWebSocket = globalThis.WebSocket;

async function waitForServerPumpTimer(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 20));
}

async function waitForFakeWebSocketNegotiation(): Promise<void> {
  for (let turn = 0; turn < 6; turn += 1) await Promise.resolve();
}

async function committedBatchId(receiptPromise: Promise<unknown>): Promise<string> {
  const receipt = (await receiptPromise) as { kind: string; batchId: Promise<string> };
  if (receipt.kind !== "committed") throw new Error("expected committed write receipt");
  return await receipt.batchId;
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
    await waitForFakeWebSocketNegotiation();
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

  it("does not emit the fake server hello before the client prelude and hello", async () => {
    const socket = new FakeWebSocket("ws://127.0.0.1:4200/apps/app-a/ws");
    const received: Uint8Array[] = [];
    socket.addEventListener("message", (event) => received.push(event.data as Uint8Array));

    socket.send(encodeWebSocketFrameBatch([Uint8Array.from([1, 42])]));
    await Promise.resolve();
    expect(received).toEqual([]);

    socket.send(encodeWebSocketPrelude("{}", new Uint8Array(16)));
    await Promise.resolve();
    expect(received).toEqual([]);

    socket.send(encodeWebSocketFrameBatch([encodeWireClientHello()]));
    await Promise.resolve();

    expect(received).toHaveLength(1);
    expect(isWireHello(decodeWebSocketFrameBatch(received[0]!)[0]!)).toBe(true);
  });

  it("retries a pending edge wait when a websocket frame arrives without a native callback", async () => {
    let settled = false;
    let transportTicks = 0;
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    transport.tick = () => {
      transportTicks += 1;
      if (transportTicks >= 2) settled = true;
      return 0;
    };
    const write = {
      batchId: "00000000000070008000000000000007",
      payload: new Uint8Array(),
      wait: () => (settled ? Promise.resolve() : new Promise<void>(() => {})),
      writeState: () => ({}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insertWithIdEncoded: () => write,
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
    await waitForFakeWebSocketNegotiation();
    await Promise.resolve();
    transportTicks = 0;

    const inserted = runtime.insert(
      "todos",
      { title: { type: "Text", value: "pump delayed fate" } },
      null,
      "00000000-0000-0000-0000-000000000007",
    );

    const wait = runtime.waitForTransaction(await committedBatchId(inserted), "edge");
    await Promise.resolve();
    await Promise.resolve();
    expect(transportTicks).toBe(1);

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([Uint8Array.from([1, 42])]));
    await wait;

    expect(transportTicks).toBe(2);
  });

  it("retries a pending edge wait when a websocket frame arrives without a native callback", async () => {
    let settled = false;
    let transportTicks = 0;
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    transport.tick = () => {
      transportTicks += 1;
      if (transportTicks >= 3) settled = true;
      return 0;
    };
    const write = {
      batchId: "00000000000070008000000000000007",
      payload: new Uint8Array(),
      wait: () => {
        if (!settled) throw new Error("transaction has not reached requested tier Edge");
      },
      writeState: () => ({}),
      nextWriteStateChange: () => new Promise<void>(() => {}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insertWithIdEncoded: () => write,
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
    await waitForFakeWebSocketNegotiation();
    await Promise.resolve();
    transportTicks = 0;

    const inserted = runtime.insert(
      "todos",
      { title: { type: "Text", value: "pump delayed fate" } },
      null,
      "00000000-0000-0000-0000-000000000007",
    );

    const wait = runtime.waitForTransaction(await committedBatchId(inserted), "edge");
    await Promise.resolve();
    await Promise.resolve();
    expect(transportTicks).toBe(2);

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([Uint8Array.from([1, 42])]));
    await wait;

    expect(transportTicks).toBe(3);
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
    await waitForFakeWebSocketNegotiation();
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
    await waitForFakeWebSocketNegotiation();
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
    await waitForFakeWebSocketNegotiation();
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
});

const testSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
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

  private sawClientPrelude = false;
  private serverHelloScheduled = false;

  constructor(readonly url: string) {}

  send(data: Uint8Array | string): void {
    this.sent.push(data);
    if (typeof data === "string") {
      this.sawClientPrelude = isClientPrelude(data);
      return;
    }
    if (!this.sawClientPrelude || this.serverHelloScheduled) return;
    if (!isClientHelloBatch(data)) return;
    this.serverHelloScheduled = true;
    queueMicrotask(() => {
      if (!this.closed) this.emitMessage(encodeWebSocketFrameBatch([encodeWireServerHello()]));
    });
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

function encodeWireServerHello(epoch: bigint = 1n): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0); // WireFrame::Hello
  writer.u64(WIRE_PROTOCOL_VERSION);
  writer.u64(WIRE_PROTOCOL_VERSION);
  writer.u64(CLIENT_WIRE_FEATURES);
  writer.u64(1); // WirePeerRole::Core
  writer.some((authority) => {
    authority.bytes(
      Uint8Array.from({ length: 16 }, () => 0x5e),
      false,
    );
    authority.u64(epoch);
  });
  return writer.finish();
}

function isClientPrelude(data: string): boolean {
  try {
    const prelude = JSON.parse(data) as { peer_identity?: unknown; auth?: unknown };
    return (
      typeof prelude.peer_identity === "string" &&
      prelude.auth !== null &&
      typeof prelude.auth === "object"
    );
  } catch {
    return false;
  }
}

function isClientHelloBatch(data: Uint8Array): boolean {
  try {
    const frames = decodeWebSocketFrameBatch(data);
    if (frames.length !== 1 || !isWireHello(frames[0]!)) return false;
    const hello = new PostcardReader(frames[0]!);
    hello.u64(); // WireFrame::Hello
    hello.u64(); // min_protocol_version
    hello.u64(); // max_protocol_version
    hello.u64(); // features
    return hello.u64() === 0; // WirePeerRole::Client
  } catch {
    return false;
  }
}
function fakeDb<T extends object>(
  db: T,
): T & { setTickScheduler(callback: (urgency: "immediate" | "deferred") => void): void } {
  type FakeOpenBatch = {
    kind: "mergeable" | "exclusive";
    author?: Uint8Array;
    tx?: TxForTest;
  };
  const implementation = db as T & {
    mergeableTx?(openBatchId: string): TxForTest;
    mergeableTxForIdentity?(openBatchId: string, author: Uint8Array): TxForTest;
    exclusiveTx?(openBatchId: string): TxForTest;
  };
  const openBatches = new Map<string, FakeOpenBatch>();
  const attach = (openBatchId: string, kind: FakeOpenBatch["kind"]): TxForTest => {
    const batch = openBatches.get(openBatchId);
    if (!batch || batch.kind !== kind) throw new Error(`unknown ${kind} batch ${openBatchId}`);
    batch.tx ??=
      kind === "exclusive"
        ? (implementation.exclusiveTx?.(openBatchId) ?? fakeTx())
        : batch.author && implementation.mergeableTxForIdentity
          ? implementation.mergeableTxForIdentity(openBatchId, batch.author)
          : (implementation.mergeableTx?.(openBatchId) ?? fakeTx());
    return batch.tx;
  };
  return {
    setTickScheduler: () => undefined,
    onMutationError: () => undefined,
    beginTransaction: (openBatchId: string, kind: FakeOpenBatch["kind"], author?: Uint8Array) => {
      openBatches.set(openBatchId, { kind, author });
    },
    attachMergeableTx: (openBatchId: string) => attach(openBatchId, "mergeable"),
    attachExclusiveTx: (openBatchId: string) => attach(openBatchId, "exclusive"),
    commitTransaction: (openBatchId: string) => {
      const batch = openBatches.get(openBatchId);
      if (!batch) throw new Error(`unknown batch ${openBatchId}`);
      openBatches.delete(openBatchId);
      return batch.tx?.commit() ?? fakeWrite();
    },
    rollbackTransaction: (openBatchId: string) => {
      const batch = openBatches.get(openBatchId);
      if (!batch) throw new Error(`unknown batch ${openBatchId}`);
      batch.tx?.rollback();
      openBatches.delete(openBatchId);
    },
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
    batchId: "00000000000070008000000000000001",
    payload: new Uint8Array(0),
    wait: async () => undefined,
    writeState: () => ({}),
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
