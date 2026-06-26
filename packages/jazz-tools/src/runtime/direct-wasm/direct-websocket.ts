import { httpUrlToWs } from "../url.js";
import { PostcardReader, PostcardWriter } from "./direct-codec.js";

export type DirectWebSocketFrameHandler = (frame: Uint8Array) => void;

export type DirectWebSocketCarrierOptions = {
  serverUrl: string;
  appId: string;
  peerIdentity: Uint8Array;
  onFrame: DirectWebSocketFrameHandler;
  WebSocket?: DirectWebSocketConstructor;
};

export type DirectWebSocketConstructor = new (url: string) => DirectBrowserWebSocket;

export type DirectBrowserWebSocket = {
  binaryType: "arraybuffer" | "blob";
  readonly readyState: number;
  send(data: Uint8Array): void;
  close(): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
  addEventListener(type: "close", listener: () => void): void;
};

export function directWebSocketUrl(
  serverUrl: string,
  appId: string,
  peerIdentity: Uint8Array,
): string {
  const url = new URL(httpUrlToWs(serverUrl, appId));
  url.searchParams.set("identity", bytesToHex(peerIdentity));
  return url.toString();
}

export function encodeDirectWebSocketFrameBatch(frames: readonly Uint8Array[]): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((itemWriter, index) => itemWriter.bytes(frames[index]!), frames.length);
  return writer.finish();
}

export function decodeDirectWebSocketFrameBatch(batch: Uint8Array): Uint8Array[] {
  const reader = new PostcardReader(batch);
  return reader.readVec((itemReader) => itemReader.bytes());
}

export class DirectWebSocketCarrier {
  readonly url: string;
  private readonly socket: DirectBrowserWebSocket;
  private readonly onFrame: DirectWebSocketFrameHandler;
  private readonly opened: Promise<void>;

  constructor(options: DirectWebSocketCarrierOptions) {
    const WebSocketCtor = options.WebSocket ?? browserWebSocketConstructor();
    this.url = directWebSocketUrl(options.serverUrl, options.appId, options.peerIdentity);
    this.onFrame = options.onFrame;
    this.socket = new WebSocketCtor(this.url);
    this.socket.binaryType = "arraybuffer";
    this.opened = waitForOpen(this.socket);
    this.socket.addEventListener("message", (event) => {
      void this.handleMessage(event.data);
    });
  }

  async send(frame: Uint8Array): Promise<void> {
    await this.ready();
    this.socket.send(encodeDirectWebSocketFrameBatch([frame]));
  }

  async sendBatch(frames: readonly Uint8Array[]): Promise<void> {
    await this.ready();
    this.socket.send(encodeDirectWebSocketFrameBatch(frames));
  }

  ready(): Promise<void> {
    return this.opened;
  }

  close(): void {
    this.socket.close();
  }

  private async handleMessage(data: unknown): Promise<void> {
    for (const frame of decodeDirectWebSocketFrameBatch(await bytesFromWebSocketMessage(data))) {
      this.onFrame(frame);
    }
  }
}

export async function connectDirectWebSocketCarrier(
  options: DirectWebSocketCarrierOptions,
): Promise<DirectWebSocketCarrier> {
  const carrier = new DirectWebSocketCarrier(options);
  await carrier.ready();
  return carrier;
}

export async function bytesFromWebSocketMessage(data: unknown): Promise<Uint8Array> {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  if (typeof Blob !== "undefined" && data instanceof Blob) {
    return new Uint8Array(await data.arrayBuffer());
  }
  throw new Error(`expected binary websocket message, got ${typeof data}`);
}

function browserWebSocketConstructor(): DirectWebSocketConstructor {
  const candidate = (globalThis as { WebSocket?: DirectWebSocketConstructor }).WebSocket;
  if (!candidate) {
    throw new Error("browser WebSocket is not available");
  }
  return candidate;
}

function waitForOpen(socket: DirectBrowserWebSocket): Promise<void> {
  if (socket.readyState === 1) return Promise.resolve();
  return new Promise((resolve, reject) => {
    let settled = false;
    const settle = (callback: () => void): void => {
      if (settled) return;
      settled = true;
      callback();
    };
    socket.addEventListener("open", () => settle(resolve));
    socket.addEventListener("error", (event) => settle(() => reject(event)));
    socket.addEventListener("close", () =>
      settle(() => reject(new Error("websocket closed before open"))),
    );
  });
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
