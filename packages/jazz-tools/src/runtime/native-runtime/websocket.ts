import { httpUrlToWs } from "../url.js";
import { mapAuthReason } from "../auth-state.js";
import type { AuthFailureReason } from "../auth-state.js";
import { PostcardReader, PostcardWriter } from "./native-codec.js";

export type WebSocketFrameHandler = (frame: Uint8Array) => void;
export type WebSocketErrorHandler = (error: WireError) => void;

export type WireError = {
  code: string;
  retry: string;
  message: string;
};

export type WebSocketCarrierOptions = {
  endpointUrl: string;
  peerIdentity: Uint8Array;
  authJson?: string;
  onFrame: WebSocketFrameHandler;
  onError?: WebSocketErrorHandler;
  WebSocket?: WebSocketConstructor;
};

export type WebSocketConstructor = new (url: string) => BrowserWebSocket;

export type BrowserWebSocket = {
  binaryType: "arraybuffer" | "blob";
  readonly readyState: number;
  send(data: Uint8Array | string): void;
  close(): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
  addEventListener(type: "close", listener: () => void): void;
};

export const WIRE_PROTOCOL_VERSION = 3;
export const MIN_WIRE_PROTOCOL_VERSION = WIRE_PROTOCOL_VERSION;
export const MAX_WIRE_PROTOCOL_VERSION = WIRE_PROTOCOL_VERSION;
export const FEATURE_SYNC_MESSAGE_PAYLOAD = 1 << 0;
export const FEATURE_STRUCTURED_ERRORS = 1 << 2;
export const FEATURE_PAYLOAD_ZSTD = 1 << 4;
export const CLIENT_WIRE_FEATURES =
  FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_PAYLOAD_ZSTD;

export function webSocketUrl(serverUrl: string, appId: string): string {
  return httpUrlToWs(serverUrl, appId);
}

export function encodeWebSocketFrameBatch(frames: readonly Uint8Array[]): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((itemWriter, index) => itemWriter.bytes(frames[index]!), frames.length);
  return writer.finish();
}

export function decodeWebSocketFrameBatch(batch: Uint8Array): Uint8Array[] {
  const reader = new PostcardReader(batch);
  return reader.readVec((itemReader) => itemReader.bytes());
}

export function encodeWireClientHello(): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0); // WireFrame::Hello
  writer.u64(MIN_WIRE_PROTOCOL_VERSION); // min_protocol_version
  writer.u64(MAX_WIRE_PROTOCOL_VERSION); // max_protocol_version
  writer.u64(CLIENT_WIRE_FEATURES);
  writer.u64(0); // WirePeerRole::Client
  return writer.finish();
}

export function isWireHello(frame: Uint8Array): boolean {
  return new PostcardReader(frame).u64() === 0;
}

export function isWireMessage(frame: Uint8Array): boolean {
  return new PostcardReader(frame).u64() === 1;
}

export function isWireError(frame: Uint8Array): boolean {
  return new PostcardReader(frame).u64() === 2;
}

export function decodeWireError(frame: Uint8Array): WireError {
  const reader = new PostcardReader(frame);
  const tag = reader.u64();
  if (tag !== 2) throw new Error(`expected WireFrame::Error, got tag ${tag}`);
  return {
    code: wireErrorCodeName(reader.u64()),
    retry: wireRetryName(reader.u64()),
    message: reader.string(),
  };
}

export function wireAuthFailureReason(error: WireError): AuthFailureReason | null {
  if (error.code !== "auth_failed") return null;
  return mapAuthReason(error.message);
}

export class WebSocketCarrier {
  readonly url: string;
  private readonly socket: BrowserWebSocket;
  private readonly onFrame: WebSocketFrameHandler;
  private readonly onError?: WebSocketErrorHandler;
  private readonly opened: Promise<void>;
  private closing = false;

  constructor(options: WebSocketCarrierOptions) {
    const WebSocketCtor = options.WebSocket ?? browserWebSocketConstructor();
    this.url = options.endpointUrl;
    this.onFrame = options.onFrame;
    this.onError = options.onError;
    this.socket = new WebSocketCtor(this.url);
    this.socket.binaryType = "arraybuffer";
    this.opened = waitForOpen(this.socket).then(() => {
      this.socket.send(encodeWebSocketPrelude(options.authJson ?? "{}", options.peerIdentity));
      this.socket.send(encodeWebSocketFrameBatch([encodeWireClientHello()]));
    });
    this.socket.addEventListener("message", (event) => {
      void this.handleMessage(event.data);
    });
    this.socket.addEventListener("error", () => {
      if (this.closing) return;
      this.onError?.({
        code: "websocket_error",
        retry: "later",
        message: "websocket transport error",
      });
    });
    this.socket.addEventListener("close", () => {
      if (this.closing) return;
      this.onError?.({
        code: "websocket_closed",
        retry: "later",
        message: "websocket closed",
      });
    });
  }

  async send(frame: Uint8Array): Promise<void> {
    await this.ready();
    this.socket.send(encodeWebSocketFrameBatch([frame]));
  }

  async sendBatch(frames: readonly Uint8Array[]): Promise<void> {
    await this.ready();
    this.socket.send(encodeWebSocketFrameBatch(frames));
  }

  ready(): Promise<void> {
    return this.opened;
  }

  close(): void {
    if (this.closing) return;
    this.closing = true;
    try {
      this.socket.close();
    } catch {
      // Node's undici WebSocket can throw while already closing; intentional
      // shutdown should not be reported as a transport failure.
    }
  }

  private async handleMessage(data: unknown): Promise<void> {
    for (const frame of decodeWebSocketFrameBatch(await bytesFromWebSocketMessage(data))) {
      if (isWireHello(frame)) continue;
      if (isWireError(frame)) {
        this.onError?.(decodeWireError(frame));
        continue;
      }
      this.onFrame(frame);
    }
  }
}

export function encodeWebSocketPrelude(authJson: string, peerIdentity: Uint8Array): string {
  const auth = JSON.parse(authJson) as Record<string, unknown>;
  const sub = authSub(auth) ?? bytesToHex(peerIdentity);
  return JSON.stringify({
    peer_identity: bytesToHex(peerIdentity),
    auth: { sub, ...auth },
    sub,
    ...auth,
  });
}

export async function connectWebSocketCarrier(
  options: WebSocketCarrierOptions,
): Promise<WebSocketCarrier> {
  const carrier = new WebSocketCarrier(options);
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

function browserWebSocketConstructor(): WebSocketConstructor {
  const candidate = (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
  if (!candidate) {
    throw new Error("browser WebSocket is not available");
  }
  return candidate;
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

function waitForOpen(socket: BrowserWebSocket): Promise<void> {
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

function authSub(auth: Record<string, unknown>): string | null {
  const directSub = auth.sub;
  if (typeof directSub === "string" && directSub.trim()) return directSub.trim();
  const jwtToken = auth.jwt_token;
  if (typeof jwtToken === "string") {
    const jwtSub = jwtSubject(jwtToken);
    if (jwtSub) return jwtSub;
  }
  const session = auth.backend_session;
  if (session && typeof session === "object") {
    const userId = (session as { user_id?: unknown }).user_id;
    if (typeof userId === "string" && userId.trim()) return userId.trim();
  }
  return null;
}

function jwtSubject(jwtToken: string): string | null {
  const parts = jwtToken.split(".");
  if (parts.length < 2) return null;
  try {
    const payload = JSON.parse(base64UrlDecode(parts[1]!)) as { sub?: unknown };
    return typeof payload.sub === "string" && payload.sub.trim() ? payload.sub.trim() : null;
  } catch {
    return null;
  }
}

function base64UrlDecode(value: string): string {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
  if (typeof atob === "function") return atob(padded);
  return Buffer.from(padded, "base64").toString("binary");
}
