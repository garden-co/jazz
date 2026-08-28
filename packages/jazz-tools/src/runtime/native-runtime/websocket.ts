import { httpUrlToWs } from "../url.js";
import { mapAuthReason } from "../auth-state.js";
import type { AuthFailureReason } from "../auth-state.js";
import { canonicalAuthorSubject, isUsableSubject } from "../author-id.js";
import { parseJwtPayload } from "../client-session.js";
import { PostcardReader, PostcardWriter } from "./native-codec.js";

export type WebSocketFrameHandler = (frame: Uint8Array) => void;
export type WebSocketErrorHandler = (error: WireError) => void;
export type WebSocketTerminalHandler = (error: WireError) => void;

export type WireError = {
  code: string;
  retry: string;
  message: string;
};

export type WebSocketNegotiation = {
  protocolVersion: number;
  features: number;
  authority?: { node: Uint8Array; epoch: bigint };
};

export type WebSocketCarrierOptions = {
  endpointUrl: string;
  peerIdentity: Uint8Array;
  authJson?: string;
  onFrame: WebSocketFrameHandler;
  onError?: WebSocketErrorHandler;
  onTerminal?: WebSocketTerminalHandler;
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
  addEventListener(
    type: "close",
    listener: (event: { code: number; reason: string }) => void,
  ): void;
};

export const WIRE_PROTOCOL_VERSION = 14;
export const MIN_WIRE_PROTOCOL_VERSION = WIRE_PROTOCOL_VERSION;
export const MAX_WIRE_PROTOCOL_VERSION = WIRE_PROTOCOL_VERSION;
export const FEATURE_SYNC_MESSAGE_PAYLOAD = 1 << 0;
export const FEATURE_STRUCTURED_ERRORS = 1 << 2;
export const FEATURE_PAYLOAD_ZSTD = 1 << 4;
export const FEATURE_MESSAGE_FRAGMENTATION = 1 << 5;
export const FEATURE_AUTHORIZATION_SCOPE_RECEIPTS = 1 << 6;
export const FEATURE_AUTHORIZATION_SCOPE_VIEWS = 1 << 7;
export const FEATURE_AUXILIARY_CHUNKS = 1 << 8;
export const CLIENT_WIRE_FEATURES =
  FEATURE_SYNC_MESSAGE_PAYLOAD |
  FEATURE_STRUCTURED_ERRORS |
  FEATURE_PAYLOAD_ZSTD |
  FEATURE_MESSAGE_FRAGMENTATION |
  FEATURE_AUTHORIZATION_SCOPE_RECEIPTS |
  FEATURE_AUTHORIZATION_SCOPE_VIEWS |
  FEATURE_AUXILIARY_CHUNKS;

// The server route accepts WebSocket messages up to one MiB. Reserve enough
// postcard framing bytes that a burst of otherwise-valid wire frames remains
// a valid WebSocket message.
const MAX_WEBSOCKET_BATCH_BYTES = 1 << 20;
const POSTCARD_FRAME_LENGTH_RESERVE = 5;
const POSTCARD_BATCH_LENGTH_RESERVE = 5;

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
  const frames = reader.readVec((itemReader) => itemReader.bytes());
  assertReaderDone(reader, "websocket frame batch");
  return frames;
}

export function encodeWireClientHello(): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0); // WireFrame::Hello
  writer.u64(MIN_WIRE_PROTOCOL_VERSION); // min_protocol_version
  writer.u64(MAX_WIRE_PROTOCOL_VERSION); // max_protocol_version
  writer.u64(CLIENT_WIRE_FEATURES);
  writer.u64(0); // WirePeerRole::Client
  // Browser carriers do not receive the authenticated session context needed
  // to validate scoped receipts. Do not self-assert an authority endpoint:
  // preserve ordinary sync and let the server fail closed for scoped features.
  writer.none(); // WireHello::authority
  return writer.finish();
}

export function isWireHello(frame: Uint8Array): boolean {
  const reader = new PostcardReader(frame);
  if (reader.u64() !== 0) return false;
  readWireHelloBodyExact(reader);
  return true;
}

export function isWireMessage(frame: Uint8Array): boolean {
  const reader = new PostcardReader(frame);
  if (reader.u64() !== 1) return false;
  reader.u64(); // protocol_version
  reader.u64(); // features
  reader.option(readWireSession);
  reader.bytes(); // semantic payload
  assertReaderDone(reader, "WireFrame::Message");
  return true;
}

export function isWireError(frame: Uint8Array): boolean {
  const reader = new PostcardReader(frame);
  if (reader.u64() !== 2) return false;
  readWireErrorBodyExact(reader);
  return true;
}

export function decodeWireError(frame: Uint8Array): WireError {
  const reader = new PostcardReader(frame);
  const tag = reader.u64();
  if (tag !== 2) throw new Error(`expected WireFrame::Error, got tag ${tag}`);
  return readWireErrorBodyExact(reader);
}

function readWireErrorBodyExact(reader: PostcardReader): WireError {
  const error = {
    code: wireErrorCodeName(reader.u64()),
    retry: wireRetryName(reader.u64()),
    message: reader.string(),
  };
  assertReaderDone(reader, "WireFrame::Error");
  return error;
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
  private readonly onTerminal?: WebSocketTerminalHandler;
  private readonly opened: Promise<WebSocketNegotiation>;
  private resolveNegotiation!: (value: WebSocketNegotiation) => void;
  private rejectNegotiation!: (reason: unknown) => void;
  private negotiated = false;
  private closing = false;
  private terminated = false;

  constructor(options: WebSocketCarrierOptions) {
    const WebSocketCtor = options.WebSocket ?? browserWebSocketConstructor();
    this.url = options.endpointUrl;
    this.onFrame = options.onFrame;
    this.onError = options.onError;
    this.onTerminal = options.onTerminal;
    this.socket = new WebSocketCtor(this.url);
    this.socket.binaryType = "arraybuffer";
    this.opened = new Promise<WebSocketNegotiation>((resolve, reject) => {
      this.resolveNegotiation = resolve;
      this.rejectNegotiation = reject;
    });
    void waitForOpen(this.socket).then(
      () => {
        this.socket.send(encodeWebSocketPrelude(options.authJson ?? "{}", options.peerIdentity));
        this.socket.send(encodeWebSocketFrameBatch([encodeWireClientHello()]));
      },
      (error) => {
        this.reportTerminal({
          code: "websocket_error",
          retry: "later",
          message: error instanceof Error ? error.message : String(error),
        });
        this.close();
      },
    );
    this.socket.addEventListener("message", (event) => {
      void this.handleMessage(event.data).catch((error) => {
        this.reportTerminal(
          {
            code: "protocol_error",
            retry: "never",
            message: error instanceof Error ? error.message : String(error),
          },
          undefined,
          false,
        );
        this.close();
      });
    });
    this.socket.addEventListener("error", () => {
      this.reportTerminal({
        code: "websocket_error",
        retry: "later",
        message: "websocket transport error",
      });
    });
    this.socket.addEventListener("close", (event) => {
      const close = websocketCloseDetails(event);
      this.reportTerminal({
        code: "websocket_closed",
        retry: "later",
        message: `websocket closed (code=${close.code ?? "unknown"}, reason=${close.reason ?? "none"})`,
      });
    });
  }

  async send(frame: Uint8Array): Promise<void> {
    await this.sendBatch([frame]);
  }

  async sendBatch(frames: readonly Uint8Array[]): Promise<void> {
    await this.ready();
    let batch: Uint8Array[] = [];
    let batchBytes = POSTCARD_BATCH_LENGTH_RESERVE;
    for (const frame of frames) {
      const frameBytes = frame.byteLength + POSTCARD_FRAME_LENGTH_RESERVE;
      if (batch.length > 0 && batchBytes + frameBytes > MAX_WEBSOCKET_BATCH_BYTES) {
        this.socket.send(encodeWebSocketFrameBatch(batch));
        batch = [];
        batchBytes = POSTCARD_BATCH_LENGTH_RESERVE;
      }
      batch.push(frame);
      batchBytes += frameBytes;
    }
    if (batch.length > 0) {
      this.socket.send(encodeWebSocketFrameBatch(batch));
    }
  }

  ready(): Promise<WebSocketNegotiation> {
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

  private reportTerminal(
    error: WireError,
    negotiationError = new Error(error.message),
    notifyError = true,
  ): void {
    if (this.closing || this.terminated) return;
    this.terminated = true;
    this.rejectNegotiation(negotiationError);
    if (notifyError) this.onError?.(error);
    this.onTerminal?.(error);
  }

  private async handleMessage(data: unknown): Promise<void> {
    for (const frame of decodeWebSocketFrameBatch(await bytesFromWebSocketMessage(data))) {
      if (this.closing) return;
      if (isWireHello(frame)) {
        if (this.negotiated) continue;
        this.negotiated = true;
        this.resolveNegotiation(decodeServerHello(frame));
        continue;
      }
      if (!this.negotiated) {
        if (isWireError(frame)) {
          const error = decodeWireError(frame);
          if (wireAuthFailureReason(error)) {
            this.reportTerminal(
              error,
              new Error(`websocket authentication failed before server hello: ${error.message}`),
            );
            this.close();
            return;
          }
        }
        this.reportTerminal(
          {
            code: "protocol_error",
            retry: "never",
            message: "websocket received semantic frame before server hello",
          },
          undefined,
          false,
        );
        this.close();
        return;
      }
      if (isWireError(frame)) {
        this.onError?.(decodeWireError(frame));
        continue;
      }
      this.onFrame(frame);
    }
  }
}

function decodeServerHello(frame: Uint8Array): WebSocketNegotiation {
  const reader = new PostcardReader(frame);
  if (reader.u64() !== 0) throw new Error("expected WireFrame::Hello");
  const hello = readWireHelloBodyExact(reader);
  const { min, max, features, role, authority } = hello;
  if (min > WIRE_PROTOCOL_VERSION || max < WIRE_PROTOCOL_VERSION) {
    throw new Error(`server does not support wire protocol ${WIRE_PROTOCOL_VERSION}`);
  }
  const unsupportedFeatures = features & ~BigInt(CLIENT_WIRE_FEATURES);
  if (unsupportedFeatures !== 0n) {
    throw new Error(`server accepted unsupported wire features 0x${features.toString(16)}`);
  }
  if (role !== 1) throw new Error("expected WirePeerRole::Core server hello");
  return { protocolVersion: WIRE_PROTOCOL_VERSION, features: Number(features), authority };
}

function readWireHelloBodyExact(reader: PostcardReader): {
  min: number;
  max: number;
  features: bigint;
  role: number;
  authority?: { node: Uint8Array; epoch: bigint };
} {
  const min = reader.u64();
  const max = reader.u64();
  const features = reader.u64BigInt();
  const role = reader.u64();
  const authority = reader.option((value) => {
    const node = value.bytes();
    if (node.byteLength !== 16) {
      throw new Error(`WireHello.authority.node must be exactly 16 bytes, got ${node.byteLength}`);
    }
    return { node, epoch: value.u64BigInt() };
  });
  assertReaderDone(reader, "WireFrame::Hello");
  return { min, max, features, role, authority };
}

function readWireSession(reader: PostcardReader): void {
  reader.string(); // session_id
  reader.u64BigInt(); // epoch
  reader.option((identity) => identity.string()); // canonical AuthorSubject
}

function assertReaderDone(reader: PostcardReader, payload: string): void {
  if (!reader.done()) throw new Error(`${payload} has trailing postcard bytes`);
}

function websocketCloseDetails(event: unknown): { code?: number; reason?: string } {
  if (!event || typeof event !== "object") return {};
  const close = event as { code?: unknown; reason?: unknown };
  return {
    code: typeof close.code === "number" ? close.code : undefined,
    reason: typeof close.reason === "string" ? close.reason : undefined,
  };
}

export function encodeWebSocketPrelude(authJson: string, peerIdentity: Uint8Array): string {
  const auth = JSON.parse(authJson) as Record<string, unknown>;
  const peerAuthor = new TextDecoder().decode(peerIdentity);
  const sub = authSub(auth) ?? canonicalAuthorSubjectPart(peerAuthor) ?? peerAuthor;
  return JSON.stringify({
    peer_identity: peerAuthor,
    ...auth,
    auth: { ...auth, sub },
    sub,
  });
}

/**
 * Pick the logical author asserted by a client WebSocket connection.
 *
 * `peerIdentity` on a native runtime is the verified logical author that
 * opened its raw-core storage handle. A WebSocket still derives the assertion
 * from the credential's full issuer and subject pair: that makes the wire
 * contract explicit and never relies on a bare `sub` or unrelated fallback.
 *
 * A credential without a usable session subject (for example an admin-only
 * connection) retains the caller's transport identity. It cannot accidentally
 * fall back to the historical bare-sub wire representation.
 */
export function peerIdentityForWebSocketAuth(
  authJson: string,
  fallbackIdentity: Uint8Array,
): Uint8Array {
  const auth = JSON.parse(authJson) as Record<string, unknown>;
  const canonical = canonicalAuthorForWebSocketAuth(auth);
  return canonical ? new TextEncoder().encode(canonical) : fallbackIdentity;
}

function canonicalAuthorForWebSocketAuth(auth: Record<string, unknown>): string | null {
  // Admin admission is intentionally sessionless and takes precedence over
  // every other field in the server route. Do not let an incidental (or
  // attacker-controlled) bearer payload change its peer identity/cap bucket.
  if (typeof auth.admin_secret === "string") return null;

  // `backend_session` is only accepted by the server together with a valid
  // backend secret. It carries the same public Session wire fields as a
  // server-side impersonation request, so it uses the same canonical author.
  // It also has the server's highest session-authentication precedence.
  const session = auth.backend_session as { issuer?: unknown; user_id?: unknown } | null;
  if (
    typeof auth.backend_secret === "string" &&
    session !== null &&
    typeof session === "object" &&
    !Array.isArray(session) &&
    typeof session.issuer === "string" &&
    typeof session.user_id === "string" &&
    isUsableSubject(session.issuer) &&
    isUsableSubject(session.user_id)
  ) {
    return canonicalAuthorSubject(session.issuer, session.user_id);
  }

  if (typeof auth.jwt_token === "string") {
    const payload = parseJwtPayload(auth.jwt_token);
    const issuer = typeof payload?.iss === "string" ? payload.iss : undefined;
    const subject = payload?.sub;
    if (
      typeof issuer === "string" &&
      typeof subject === "string" &&
      isUsableSubject(issuer) &&
      isUsableSubject(subject)
    ) {
      return canonicalAuthorSubject(issuer, subject);
    }
  }

  return null;
}

function canonicalAuthorSubjectPart(author: string): string | null {
  try {
    const parsed = JSON.parse(author) as unknown;
    return Array.isArray(parsed) && parsed.length === 2 && typeof parsed[1] === "string"
      ? parsed[1]
      : null;
  } catch {
    return null;
  }
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

function authSub(auth: Record<string, unknown>): string | null {
  const directSub = auth.sub;
  if (typeof directSub === "string" && isUsableSubject(directSub)) return directSub;
  const jwtToken = auth.jwt_token;
  if (typeof jwtToken === "string") {
    const jwtSub = jwtSubject(jwtToken);
    if (jwtSub) return jwtSub;
  }
  const session = auth.backend_session;
  if (session && typeof session === "object") {
    const userId = (session as { user_id?: unknown }).user_id;
    if (typeof userId === "string" && isUsableSubject(userId)) return userId;
  }
  return null;
}

function jwtSubject(jwtToken: string): string | null {
  const parts = jwtToken.split(".");
  if (parts.length < 2) return null;
  try {
    const payload = JSON.parse(base64UrlDecode(parts[1]!)) as { sub?: unknown };
    return typeof payload.sub === "string" && isUsableSubject(payload.sub) ? payload.sub : null;
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
