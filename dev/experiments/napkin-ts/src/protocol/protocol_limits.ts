import { Writer } from "../support/postcard.js";
import {
  type RegisterShapeOptions,
  type ShapeAst,
  writeRegisterShapeOptions,
  writeShapeAst,
} from "./protocol/keys.js";
import type { VersionRecord } from "./protocol/versions.js";
export {
  MAX_POLICY_EXPRESSION_DEPTH,
  MAX_POLICY_EXPRESSION_NODES,
} from "../_extern/types-protocol.js";

export const MAX_COMMIT_UNIT_VERSIONS = 4096;
export const MAX_WIRE_FRAME_BYTES = 2 * 1024 * 1024;
export const MAX_WIRE_BATCH_FRAMES = MAX_COMMIT_UNIT_VERSIONS;
export const MAX_LOGICAL_MESSAGE_BYTES = 256 * 1024 * 1024;
export const MAX_ENCODED_MESSAGE_BYTES =
  MAX_LOGICAL_MESSAGE_BYTES + Math.floor(MAX_LOGICAL_MESSAGE_BYTES / 10) + 24;
export const MAX_FRAGMENT_REASSEMBLY_IDLE_MS = 30_000;
export const MAX_FRAGMENT_REASSEMBLY_AGE_MS = 5 * 60 * 1_000;
export const MAX_SHAPE_AST_BYTES = 64 * 1024;
export const MAX_SHAPE_REGISTRATION_BYTES = MAX_SHAPE_AST_BYTES;
export const MAX_SHAPE_REGISTRATIONS_PER_PEER = 1024;
export const MAX_RETAINED_PEER_SHAPES = 1024;
export const MAX_KNOWN_STATE_EXACT_REFS = 1024;
export const MAX_CHUNK_REQUEST_BATCH_ENTRIES = 4;

export const validateWireFrameLen = (len: number) =>
  validateLen("wire frame", len, MAX_WIRE_FRAME_BYTES);
export const validateLogicalMessageLen = (len: number) =>
  validateLen("logical message payload", len, MAX_LOGICAL_MESSAGE_BYTES);

function measure(what: string, write: (w: Writer) => void): number {
  const w = new Writer();
  try {
    write(w);
  } catch (e) {
    throw new Error(`failed to measure ${what} payload: ${(e as Error).message}`);
  }
  return w.finish().length;
}

export function validateShapeAstSize(ast: ShapeAst): void {
  validateLen(
    "shape AST",
    measure("shape AST", (w) => writeShapeAst(w, ast)),
    MAX_SHAPE_AST_BYTES,
  );
}

export function validateShapeRegistrationSize(ast: ShapeAst, opts: RegisterShapeOptions): void {
  const size = measure("shape registration", (w) => {
    writeShapeAst(w, ast);
    writeRegisterShapeOptions(w, opts);
  });
  validateLen("shape registration", size, MAX_SHAPE_REGISTRATION_BYTES);
}

export function commitUnitLimitViolation(versions: VersionRecord[]): string | undefined {
  if (versions.length > MAX_COMMIT_UNIT_VERSIONS)
    return `commit unit version count ${versions.length} exceeds max ${MAX_COMMIT_UNIT_VERSIONS}`;
  return undefined;
}

function validateLen(label: string, len: number, max: number): void {
  if (len > max) throw new Error(`${label} size ${len} exceeds max ${max}`);
}
