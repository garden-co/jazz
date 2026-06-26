import { PostcardReader, PostcardWriter } from "./direct-codec.js";

export type MinimalWebSocket = {
  binaryType: "arraybuffer";
  readonly readyState: number;
  send(data: string | Uint8Array): void;
  close(): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
  addEventListener(type: "close", listener: () => void): void;
};

export type WebSocketConstructor = new (url: string) => MinimalWebSocket;

export function webSocketConstructor(unavailableMessage: string): WebSocketConstructor {
  const candidate = (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
  if (!candidate) {
    throw new Error(unavailableMessage);
  }
  return candidate;
}

export async function connectWebSocket(
  WebSocketCtor: WebSocketConstructor,
  url: string,
): Promise<MinimalWebSocket> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const socket = new WebSocketCtor(url);
    socket.binaryType = "arraybuffer";
    try {
      await waitForOpen(socket);
      return socket;
    } catch (error) {
      lastError = error;
      try {
        socket.close();
      } catch {
        // Ignore close failures from sockets that never opened.
      }
      await delay(50 * (attempt + 1));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

export function websocketTransportUrl(baseUrl: string, identity?: Uint8Array): string {
  const url = new URL(baseUrl);
  if (identity) url.searchParams.set("identity", hex(identity));
  return url.toString();
}

export function encodeFrameBatch(frames: Uint8Array[]): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((itemWriter, index) => itemWriter.bytes(frames[index]), frames.length);
  return writer.finish();
}

export function decodeFrameBatch(batch: Uint8Array): Uint8Array[] {
  const reader = new PostcardReader(batch);
  return reader.readVec((itemReader) => itemReader.bytes());
}

export async function bytesFromWebSocketMessage(data: unknown): Promise<Uint8Array> {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data))
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (typeof Blob !== "undefined" && data instanceof Blob)
    return new Uint8Array(await data.arrayBuffer());
  throw new Error(`expected binary websocket message, got ${typeof data}`);
}

function waitForOpen(socket: MinimalWebSocket): Promise<void> {
  if (socket.readyState === 1) return Promise.resolve();
  return new Promise((resolve, reject) => {
    let settled = false;
    const settle = (callback: () => void): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      callback();
    };
    const timer = setTimeout(() => {
      settle(() => reject(new Error("timed out waiting for websocket open")));
    }, 5_000);
    socket.addEventListener("open", () => settle(resolve));
    socket.addEventListener("error", (event) => settle(() => reject(event)));
    socket.addEventListener("close", () =>
      settle(() => reject(new Error("websocket closed before open"))),
    );
  });
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
