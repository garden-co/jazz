import { afterEach, describe, expect, it } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import {
  decodeDirectWebSocketFrameBatch,
  encodeDirectWebSocketPrelude,
  isDirectWireHello,
} from "./direct-websocket.js";
import { DirectWasmRuntime, type DirectTransport } from "./runtime.js";

const previousWebSocket = globalThis.WebSocket;

describe("DirectWasmRuntime server transport", () => {
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
    const runtime = new DirectWasmRuntime(
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

  it("uses the caller-supplied table for update and delete", () => {
    const calls: unknown[] = [];
    const write = {
      payload: new Uint8Array(),
      wait: () => undefined,
      writeState: () => ({}),
    };
    const runtime = new DirectWasmRuntime(
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
});

const testSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;

class FakeTransport implements DirectTransport {
  closed = false;

  constructor(private readonly outgoing: Uint8Array[]) {}

  close(): boolean {
    this.closed = true;
    return true;
  }

  recvWireFrames(): unknown[] {
    return this.outgoing.splice(0);
  }

  sendWireFrame(_frame: Uint8Array): void {}

  tick(): number {
    return 0;
  }
}

class FakeWebSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readonly readyState = 1;
  readonly sent: Uint8Array[] = [];
  closed = false;

  constructor(readonly url: string) {}

  send(data: Uint8Array): void {
    this.sent.push(data);
  }

  close(): void {
    this.closed = true;
  }

  addEventListener(_type: string, _listener: (event?: unknown) => void): void {}
}
