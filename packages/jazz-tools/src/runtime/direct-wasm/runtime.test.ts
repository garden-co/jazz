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

    runtime.disconnect();

    expect(transport.closed).toBe(true);
    expect(sockets[0]!.closed).toBe(true);
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
