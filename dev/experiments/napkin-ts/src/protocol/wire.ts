// Versioned transport frames around the semantic sync protocol.
import { Reader, Writer, PostcardError } from "../support/postcard.js";
import { blake3 } from "../support/hash.js";
import { toHex, utf8 } from "../support/bytes.js";
import { AuthorSubject, readUuid, writeUuid, type NodeUuid } from "../types/ids.js";
import { decodePostcardExact, encodesExactly } from "../types/postcard_exact.js";
import {
  MAX_WIRE_BATCH_FRAMES,
  validateLogicalMessageLen,
  validateWireFrameLen,
} from "./protocol_limits.js";
import {
  readSyncMessage,
  requiredWireFeatures,
  validateWireContract,
  writeSyncMessage,
  type SyncMessage,
} from "./protocol.js";
import {
  CHANNEL_CLASSES,
  readChannelFrame,
  writeChannelFrame,
  type ChannelClass,
  type ChannelFrame,
} from "./wire/channels.js";
import { OrderedChannelBackend } from "./wire/stream_backend.js";

export * as channel_credit from "./wire/channel_credit.js";
export * as channels from "./wire/channels.js";
export * as stream_backend from "./wire/stream_backend.js";
export { decodePostcardExact, encodesExactly };

export const WIRE_PROTOCOL_VERSION = 6;
export type WireFeatures = bigint;
export const FEATURE_NONE: WireFeatures = 0n;
export const FEATURE_PAYLOAD_LZ4: WireFeatures = 1n << 3n;
export const FEATURE_PAYLOAD_ZSTD: WireFeatures = 1n << 4n;
export const FEATURE_AUTHORIZATION_SCOPE_VIEWS: WireFeatures = 1n << 7n;
export const FEATURE_AUXILIARY_CHUNKS: WireFeatures = 1n << 8n;
export const FEATURE_SCOPE_ISOLATED_CLIENT_RELAY: WireFeatures = 1n << 9n;

export type WireCreditKind =
  | { kind: "Frames" }
  | { kind: "Messages"; count: number; bulk: boolean };
export interface WireSession {
  sessionId: string;
  epoch: number;
  identity?: AuthorSubject;
}
export interface WireChannelCredit {
  protocolVersion: number;
  features: WireFeatures;
  session?: WireSession;
  class: ChannelClass;
  sequence: number;
  consumedBytes: number;
  kind: WireCreditKind;
}
export interface WireChannelEnvelope {
  protocolVersion: number;
  features: WireFeatures;
  session?: WireSession;
  extent: ChannelFrame;
}
export type WirePeerRole = "Client" | "Core" | "Relay";
const ROLES: WirePeerRole[] = ["Client", "Core", "Relay"];
export interface WireAuthorityEndpoint {
  node: NodeUuid;
  epoch: bigint;
}
export interface WireHello {
  minProtocolVersion: number;
  maxProtocolVersion: number;
  features: WireFeatures;
  role: WirePeerRole;
  authority?: WireAuthorityEndpoint;
}
export type WireFrame =
  | { kind: "Hello"; hello: WireHello }
  | { kind: "Error"; error: WireError }
  | { kind: "Channel"; envelope: WireChannelEnvelope }
  | { kind: "ChannelCredit"; credit: WireChannelCredit };

export function freshAuthorityEndpoint(node: NodeUuid): WireAuthorityEndpoint {
  for (;;) {
    const epoch = new DataView(crypto.getRandomValues(new Uint8Array(8)).buffer).getBigUint64(0);
    if (epoch !== 0n) return { node, epoch };
  }
}

export const currentHello = (role: WirePeerRole, features: WireFeatures): WireHello => ({
  minProtocolVersion: WIRE_PROTOCOL_VERSION,
  maxProtocolVersion: WIRE_PROTOCOL_VERSION,
  features,
  role,
});

export interface WireNegotiated {
  protocolVersion: number;
  features: WireFeatures;
}

export type WireCompression = "None" | "Lz4" | "Zstd";
export function compressionFromFeatures(features: WireFeatures): WireCompression {
  if (features & FEATURE_PAYLOAD_LZ4) return "Lz4";
  if (features & FEATURE_PAYLOAD_ZSTD) return "Zstd";
  return "None";
}
export const compressionFeature = (c: WireCompression): WireFeatures =>
  c === "Lz4" ? FEATURE_PAYLOAD_LZ4 : c === "Zstd" ? FEATURE_PAYLOAD_ZSTD : FEATURE_NONE;

/** Debug view that never prints the raw session id. */
export function describeWireSession(s: WireSession): string {
  const fp = toHex(blake3(utf8(s.sessionId))).slice(0, 12);
  const idFp = s.identity && toHex(blake3(utf8(s.identity.canonical()))).slice(0, 12);
  return `WireSession { session_id_len: ${s.sessionId.length}, session_id_fingerprint: "${fp}", epoch: ${s.epoch}, identity_fingerprint: ${idFp ? `Some("${idFp}")` : "None"} }`;
}

export type WireErrorCode =
  | "UnsupportedProtocolVersion"
  | "UnsupportedFeature"
  | "MalformedFrame"
  | "AuthFailed"
  | "Backpressure"
  | "Internal"
  | "NotReady";
const ERROR_CODES: WireErrorCode[] = [
  "UnsupportedProtocolVersion",
  "UnsupportedFeature",
  "MalformedFrame",
  "AuthFailed",
  "Backpressure",
  "Internal",
  "NotReady",
];
export type WireRetry = "Never" | "AfterAuth" | "AfterResume" | "Later";
const RETRIES: WireRetry[] = ["Never", "AfterAuth", "AfterResume", "Later"];

export class WireError extends Error {
  constructor(
    readonly code: WireErrorCode,
    readonly retry: WireRetry,
    message: string,
  ) {
    super(message);
  }
  asksReconnectLater(): boolean {
    return this.retry === "Later" && (this.code === "Backpressure" || this.code === "NotReady");
  }
}

// ---- frame codec ----

const writeSession = (w: Writer, s: WireSession) => {
  w.str(s.sessionId).varint(s.epoch);
  w.option(s.identity, AuthorSubject.write);
};
const readSession = (r: Reader): WireSession => ({
  sessionId: r.str(),
  epoch: r.varint(),
  identity: r.option(AuthorSubject.read),
});
const writeClass = (w: Writer, c: ChannelClass) => w.variant(CHANNEL_CLASSES.indexOf(c));
const readClass = (r: Reader) => CHANNEL_CLASSES[r.variant(7, "ChannelClass")];

export function writeWireFrame(w: Writer, f: WireFrame): void {
  switch (f.kind) {
    case "Hello": {
      const h = f.hello;
      w.variant(0).varint(h.minProtocolVersion).varint(h.maxProtocolVersion).varint64(h.features);
      w.variant(ROLES.indexOf(h.role));
      w.option(h.authority, (w, a) => {
        writeUuid(w, a.node);
        w.varint64(a.epoch);
      });
      break;
    }
    case "Error":
      w.variant(1)
        .variant(ERROR_CODES.indexOf(f.error.code))
        .variant(RETRIES.indexOf(f.error.retry))
        .str(f.error.message);
      break;
    case "Channel": {
      const e = f.envelope;
      w.variant(2).varint(e.protocolVersion).varint64(e.features);
      w.option(e.session, writeSession);
      writeChannelFrame(w, e.extent);
      break;
    }
    case "ChannelCredit": {
      const c = f.credit;
      w.variant(3).varint(c.protocolVersion).varint64(c.features);
      w.option(c.session, writeSession);
      writeClass(w, c.class);
      w.varint(c.sequence).varint(c.consumedBytes);
      if (c.kind.kind === "Frames") w.variant(0);
      else w.variant(1).varint(c.kind.count).bool(c.kind.bulk);
      break;
    }
  }
}

export function readWireFrame(r: Reader): WireFrame {
  switch (r.variant(4, "WireFrame")) {
    case 0:
      return {
        kind: "Hello",
        hello: {
          minProtocolVersion: r.varint(),
          maxProtocolVersion: r.varint(),
          features: r.varint64(),
          role: ROLES[r.variant(3, "WirePeerRole")],
          authority: r.option((r) => ({ node: readUuid(r), epoch: r.varint64() })),
        },
      };
    case 1:
      return {
        kind: "Error",
        error: new WireError(
          ERROR_CODES[r.variant(7, "WireErrorCode")],
          RETRIES[r.variant(4, "WireRetry")],
          r.str(),
        ),
      };
    case 2:
      return {
        kind: "Channel",
        envelope: {
          protocolVersion: r.varint(),
          features: r.varint64(),
          session: r.option(readSession),
          extent: readChannelFrame(r),
        },
      };
    default: {
      const protocolVersion = r.varint();
      const features = r.varint64();
      const session = r.option(readSession);
      const cls = readClass(r);
      const sequence = r.varint();
      const consumedBytes = r.varint();
      const kind: WireCreditKind =
        r.variant(2, "WireCreditKind") === 0
          ? { kind: "Frames" }
          : { kind: "Messages", count: r.varint(), bulk: r.bool() };
      return {
        kind: "ChannelCredit",
        credit: { protocolVersion, features, session, class: cls, sequence, consumedBytes, kind },
      };
    }
  }
}

export function encodeFrame(frame: WireFrame): Uint8Array {
  const w = new Writer();
  writeWireFrame(w, frame);
  return w.finish();
}

export const isChannelCreditFrame = (bytes: Uint8Array) => bytes[0] === 3;

const END = "Hit the end of buffer, expected more data";
export function decodeFrame(bytes: Uint8Array): WireFrame {
  try {
    validateWireFrameLen(bytes.length);
  } catch {
    throw new PostcardError(END);
  }
  return decodePostcardExact(bytes, readWireFrame, writeWireFrame);
}

function decodeFrameTrusted(bytes: Uint8Array): WireFrame {
  return readWireFrame(new Reader(bytes));
}

/** Validates the session/version/feature metadata of inbound frames. */
export class WireInboundContext {
  private trustedEncoder = false;
  constructor(
    readonly expectedProtocolVersion: number,
    readonly negotiatedFeatures: WireFeatures,
    readonly expectedSession?: WireSession,
  ) {}

  clone(): WireInboundContext {
    const c = new WireInboundContext(
      this.expectedProtocolVersion,
      this.negotiatedFeatures,
      this.expectedSession,
    );
    c.trustedEncoder = this.trustedEncoder;
    return c;
  }
  setTrustedEncoder(trusted: boolean) {
    this.trustedEncoder = trusted;
  }
  validatesReceipts(): boolean {
    return !this.trustedEncoder;
  }

  decodeSemanticPayload(bytes: Uint8Array): SyncMessage {
    if (!this.trustedEncoder) return decodeSyncMessageForFeatures(bytes, this.negotiatedFeatures);
    let message: SyncMessage;
    try {
      message = decodeSyncMessageTrusted(bytes);
    } catch (e) {
      throw new WireError("MalformedFrame", "Never", (e as Error).message);
    }
    ensureSyncMessageFeatures(message, this.negotiatedFeatures);
    return message;
  }

  validateChannelMetadata(frame: WireChannelEnvelope): void {
    this.validateMetadata(frame.protocolVersion, frame.features, frame.session);
  }

  decodeFrame(bytes: Uint8Array): WireFrame {
    return this.trustedEncoder ? decodeFrameTrusted(bytes) : decodeFrame(bytes);
  }

  validateMetadata(
    protocolVersion: number,
    features: WireFeatures,
    session: WireSession | undefined,
  ): void {
    if (protocolVersion !== this.expectedProtocolVersion)
      throw new WireError(
        "UnsupportedProtocolVersion",
        "AfterResume",
        `wire message protocol version ${protocolVersion} does not match negotiated ${this.expectedProtocolVersion}`,
      );
    const unnegotiated = features & ~this.negotiatedFeatures;
    if (unnegotiated !== 0n)
      throw new WireError(
        "UnsupportedFeature",
        "AfterResume",
        `wire message declares unnegotiated features 0x${unnegotiated.toString(16)}`,
      );
    const expected = this.expectedSession;
    if (!expected) return;
    if (!session) throw new WireError("AuthFailed", "AfterAuth", "missing wire session metadata");
    if (session.sessionId !== expected.sessionId)
      throw new WireError(
        "AuthFailed",
        "AfterResume",
        "wire session id does not match this connection",
      );
    const sameIdentity =
      session.identity === undefined || expected.identity === undefined
        ? session.identity === expected.identity
        : session.identity.equals(expected.identity);
    if (!sameIdentity)
      throw new WireError(
        "AuthFailed",
        "AfterAuth",
        "wire session identity does not match this connection",
      );
    if (session.epoch < expected.epoch)
      throw new WireError("AuthFailed", "AfterResume", "stale wire session epoch");
    if (session.epoch !== expected.epoch)
      throw new WireError(
        "AuthFailed",
        "AfterResume",
        "wire session epoch does not match this connection",
      );
  }
}

export function validateFrameForArtifactCorpus(bytes: Uint8Array, features: WireFeatures): void {
  validateFramesForArtifactCorpus([bytes], features);
}

export function validateFramesForArtifactCorpus(
  frames: Uint8Array[],
  features: WireFeatures,
): void {
  const context = new WireInboundContext(WIRE_PROTOCOL_VERSION, features);
  const receiver = new OrderedChannelBackend(context.clone());
  const pending = new Set<number>();
  for (const bytes of frames) {
    if (wireHelloVersionRange(bytes)) {
      try {
        negotiateWire(decodeHelloFrame(bytes), features);
      } catch (e) {
        throw new Error(`hello negotiation rejected: ${(e as Error).message}`);
      }
      continue;
    }
    let frame: WireFrame;
    try {
      frame = decodeFrame(bytes);
    } catch (e) {
      throw new Error(`malformed wire frame: ${(e as Error).message}`);
    }
    switch (frame.kind) {
      case "Hello":
        try {
          negotiateWire(frame.hello, features);
        } catch (e) {
          throw new Error(`hello negotiation rejected: ${(e as Error).message}`);
        }
        break;
      case "Channel": {
        const channel = frame.envelope.extent.channel;
        pending.add(channel);
        const message = receiver.receive(frame.envelope, bytes.length);
        if (message) {
          try {
            context.decodeSemanticPayload(message.payload);
          } catch (e) {
            throw new Error(`semantic payload rejected: ${(e as Error).message}`);
          }
          pending.delete(channel);
        }
        break;
      }
      case "Error":
        break;
      case "ChannelCredit":
        throw new Error("credit frames require a persistent admitted endpoint");
    }
  }
  if (pending.size) throw new Error("incomplete channel message in artifact corpus");
}

// ---- semantic payloads ----

export function encodeSyncMessage(message: SyncMessage): Uint8Array {
  const w = new Writer();
  writeSyncMessage(w, message);
  return w.finish();
}

export const encodedSyncMessageLen = (message: SyncMessage): number =>
  encodeSyncMessage(message).length;

export function encodeSyncMessageForFeatures(
  message: SyncMessage,
  features: WireFeatures,
): Uint8Array {
  ensureSyncMessageFeatures(message, features);
  try {
    return encodeSyncMessage(message);
  } catch (e) {
    throw new WireError(
      "MalformedFrame",
      "Never",
      `failed to encode sync message payload: ${(e as Error).message}`,
    );
  }
}

export function decodeSyncMessage(bytes: Uint8Array): SyncMessage {
  try {
    validateLogicalMessageLen(bytes.length);
  } catch {
    throw new PostcardError(END);
  }
  const message = decodePostcardExact(bytes, readSyncMessage, writeSyncMessage);
  try {
    validateWireContract(message);
  } catch {
    throw new PostcardError("Found an Option discriminant that wasn't 0 or 1");
  }
  return message;
}

export const decodeSyncMessageTrusted = (bytes: Uint8Array): SyncMessage =>
  readSyncMessage(new Reader(bytes));

export function decodeWebsocketFrameBatch(bytes: Uint8Array): Uint8Array[] {
  try {
    validateWireFrameLen(bytes.length);
  } catch {
    throw new PostcardError(END);
  }
  const r = new Reader(bytes);
  const count = takeCanonicalUsize(r, bytes);
  if (count === 0 || count > MAX_WIRE_BATCH_FRAMES)
    throw new PostcardError("The original data was not well encoded");
  const frames: Uint8Array[] = [];
  for (let i = 0; i < count; i++) {
    const len = takeCanonicalUsize(r, bytes);
    try {
      validateWireFrameLen(len);
    } catch {
      throw new PostcardError(END);
    }
    if (len > r.remaining) throw new PostcardError(END);
    frames.push(r.raw(len).slice());
  }
  if (r.remaining !== 0) throw new PostcardError("The original data was not well encoded");
  return frames;
}

export function encodeWebsocketFrameBatch(frames: Uint8Array[]): Uint8Array {
  const bad = () => new PostcardError("The serialize buffer is full");
  if (!frames.length || frames.length > MAX_WIRE_BATCH_FRAMES) throw bad();
  for (const f of frames) {
    try {
      validateWireFrameLen(f.length);
    } catch {
      throw bad();
    }
  }
  const w = new Writer();
  w.seq(frames, (w, f) => w.bytes(f));
  const encoded = w.finish();
  try {
    validateWireFrameLen(encoded.length);
  } catch {
    throw bad();
  }
  return encoded;
}

function takeCanonicalUsize(r: Reader, source: Uint8Array): number {
  const start = source.length - r.remaining;
  const value = r.varint();
  const consumed = source.subarray(start, source.length - r.remaining);
  if (!encodesExactly(value, (w, v) => w.varint(v), consumed))
    throw new PostcardError("The original data was not well encoded");
  return value;
}

export function decodeSyncMessageForFeatures(
  bytes: Uint8Array,
  features: WireFeatures,
): SyncMessage {
  let message: SyncMessage;
  try {
    message = decodeSyncMessage(bytes);
  } catch (e) {
    throw new WireError(
      "MalformedFrame",
      "Never",
      `failed to decode sync message payload: ${(e as Error).message}`,
    );
  }
  ensureSyncMessageFeatures(message, features);
  return message;
}

export const decodeSyncMessageForReceive = decodeSyncMessage;

export function ensureSyncMessageFeatures(message: SyncMessage, features: WireFeatures): void {
  const missing = requiredWireFeatures(message) & ~features;
  if (missing === 0n) return;
  throw new WireError(
    "UnsupportedFeature",
    "AfterResume",
    `sync message requires unnegotiated features 0x${missing.toString(16)}`,
  );
}

/** Browser builds have no process environment: default compression only (ruzstd decode). */
export const runtimeTransportCompressionFeatures = (): WireFeatures => FEATURE_PAYLOAD_ZSTD;

export const currentWireFeatures = (): WireFeatures =>
  FEATURE_AUTHORIZATION_SCOPE_VIEWS |
  FEATURE_AUXILIARY_CHUNKS |
  FEATURE_SCOPE_ISOLATED_CLIENT_RELAY |
  runtimeTransportCompressionFeatures();

export interface WireTransport {
  sendFrame(frame: Uint8Array): void;
  tryRecvFrame(): Uint8Array | undefined;
}
export class TransportError extends Error {
  constructor(
    readonly kind: "Backpressure" | "Failed",
    detail = "",
  ) {
    super(kind === "Backpressure" ? "Backpressure" : `Failed(${JSON.stringify(detail)})`);
  }
}

function checkWireProtocolRange(min: number, max: number): void {
  if (min === WIRE_PROTOCOL_VERSION && max === WIRE_PROTOCOL_VERSION) return;
  throw new WireError(
    "UnsupportedProtocolVersion",
    "Never",
    `unsupported wire protocol advertisement: remote ${min}..=${max}, expected ${WIRE_PROTOCOL_VERSION}..=${WIRE_PROTOCOL_VERSION}`,
  );
}

export function wireHelloVersionRange(bytes: Uint8Array): [number, number] | undefined {
  try {
    const r = new Reader(bytes);
    if (r.varint() !== 0) return undefined;
    const min = r.varint();
    const max = r.varint();
    if (min > 0xffff || max > 0xffff) return undefined;
    return [min, max];
  } catch {
    return undefined;
  }
}

export function decodeHelloFrame(bytes: Uint8Array): WireHello {
  const range = wireHelloVersionRange(bytes);
  if (range) checkWireProtocolRange(range[0], range[1]);
  let frame: WireFrame;
  try {
    frame = decodeFrame(bytes);
  } catch (e) {
    throw new WireError(
      "MalformedFrame",
      "Never",
      `malformed WireFrame::Hello: ${(e as Error).message}`,
    );
  }
  if (frame.kind !== "Hello")
    throw new WireError(
      "MalformedFrame",
      "Never",
      "expected the first wire frame to be WireFrame::Hello",
    );
  return frame.hello;
}

export function negotiateWire(remote: WireHello, localFeatures: WireFeatures): WireNegotiated {
  checkWireProtocolRange(remote.minProtocolVersion, remote.maxProtocolVersion);
  return { protocolVersion: WIRE_PROTOCOL_VERSION, features: remote.features & localFeatures };
}
