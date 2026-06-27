import { httpUrlToWs } from "../url.js";
import { mapAuthReason } from "../auth-state.js";
import type { AuthFailureReason } from "../auth-state.js";
import { PostcardReader, PostcardWriter } from "./direct-codec.js";

export type DirectWebSocketFrameHandler = (frame: Uint8Array) => void;
export type DirectWebSocketErrorHandler = (error: DirectWireError) => void;

export type DirectWireError = {
  code: string;
  retry: string;
  message: string;
};

export type DirectWebSocketCarrierOptions = {
  serverUrl?: string;
  endpointUrl?: string;
  appId?: string;
  peerIdentity: Uint8Array;
  authJson?: string;
  onFrame: DirectWebSocketFrameHandler;
  onError?: DirectWebSocketErrorHandler;
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

export function directWebSocketUrl(serverUrl: string, appId: string): string {
  return httpUrlToWs(serverUrl, appId);
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

export function encodeDirectWireClientHello(): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0); // WireFrame::Hello
  writer.u64(1); // min_protocol_version
  writer.u64(1); // max_protocol_version
  writer.u64(5); // FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS
  writer.u64(0); // WirePeerRole::Client
  return writer.finish();
}

export function isDirectWireHello(frame: Uint8Array): boolean {
  return new PostcardReader(frame).u64() === 0;
}

export function isDirectWireMessage(frame: Uint8Array): boolean {
  return new PostcardReader(frame).u64() === 1;
}

export function isDirectWireError(frame: Uint8Array): boolean {
  return new PostcardReader(frame).u64() === 2;
}

export function decodeDirectWireError(frame: Uint8Array): DirectWireError {
  const reader = new PostcardReader(frame);
  const tag = reader.u64();
  if (tag !== 2) throw new Error(`expected WireFrame::Error, got tag ${tag}`);
  return {
    code: wireErrorCodeName(reader.u64()),
    retry: wireRetryName(reader.u64()),
    message: reader.string(),
  };
}

export function directWireAuthFailureReason(error: DirectWireError): AuthFailureReason | null {
  if (error.code !== "auth_failed") return null;
  return mapAuthReason(error.message);
}

export class DirectWebSocketCarrier {
  readonly url: string;
  private readonly socket: DirectBrowserWebSocket;
  private readonly onFrame: DirectWebSocketFrameHandler;
  private readonly onError?: DirectWebSocketErrorHandler;
  private readonly opened: Promise<void>;

  constructor(options: DirectWebSocketCarrierOptions) {
    const WebSocketCtor = options.WebSocket ?? browserWebSocketConstructor();
    this.url = options.endpointUrl
      ? options.endpointUrl
      : directWebSocketUrl(
          required(options.serverUrl, "serverUrl"),
          required(options.appId, "appId"),
        );
    this.onFrame = options.onFrame;
    this.onError = options.onError;
    this.socket = new WebSocketCtor(this.url);
    this.socket.binaryType = "arraybuffer";
    this.opened = waitForOpen(this.socket).then(() => {
      this.socket.send(
        encodeDirectWebSocketPrelude(options.authJson ?? "{}", options.peerIdentity),
      );
      this.socket.send(encodeDirectWebSocketFrameBatch([encodeDirectWireClientHello()]));
    });
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
      if (isDirectWireHello(frame)) continue;
      if (isDirectWireError(frame)) {
        this.onError?.(decodeDirectWireError(frame));
        continue;
      }
      this.onFrame(frame);
    }
  }
}

export function encodeDirectWebSocketPrelude(
  authJson: string,
  peerIdentity: Uint8Array,
): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify({
      peer_identity: bytesToHex(peerIdentity),
      auth: JSON.parse(authJson) as unknown,
    }),
  );
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

function required(value: string | undefined, name: string): string {
  if (value == null) throw new Error(`DirectWebSocketCarrier requires ${name}`);
  return value;
}

function wireErrorCodeName(tag: number): string {
  switch (tag) {
    case 0:
      return "unsupported_protocol_version";
    case 1:
      return "unsupported_feature";
    case 2:
      return "malformed_frame";
    case 3:
      return "auth_failed";
    case 4:
      return "backpressure";
    case 5:
      return "internal";
    default:
      return `unknown_${tag}`;
  }
}

function wireRetryName(tag: number): string {
  switch (tag) {
    case 0:
      return "never";
    case 1:
      return "after_auth";
    case 2:
      return "after_resume";
    case 3:
      return "later";
    default:
      return `unknown_${tag}`;
  }
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
