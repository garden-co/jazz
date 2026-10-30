/**
 * Thin client frames (SPEC 8.14): `WireFrame::ThinRequest` and
 * `WireFrame::ThinResponse`, in the postcard layout of
 * `crates/jazz/layers/protocol/src/thin.rs`.
 */
import { PostcardReader, PostcardWriter } from "../runtime/native-runtime/native-codec.js";

/** `FEATURE_THIN_RESULTS` in `crates/jazz/layers/protocol/src/wire.rs`. */
export const FEATURE_THIN_RESULTS = 1 << 10;

const WIRE_FRAME_THIN_REQUEST = 4;
const WIRE_FRAME_THIN_RESPONSE = 5;

export type ThinReadTier = "local-first" | "remote";

export type ThinOp =
  | { type: "schema"; schema: Uint8Array }
  | { type: "subscribe"; query: Uint8Array; includeDeleted: boolean; tier: ThinReadTier }
  | { type: "unsubscribe"; subscription: number }
  | { type: "query"; query: Uint8Array; includeDeleted: boolean; tier: ThinReadTier }
  | {
      type: "insert";
      table: string;
      row?: Uint8Array;
      cells: Uint8Array;
      updatedAtMs?: number;
    }
  | { type: "update"; table: string; row: Uint8Array; patch: Uint8Array; updatedAtMs?: number }
  | { type: "upsert"; table: string; row: Uint8Array; cells: Uint8Array; updatedAtMs?: number }
  | { type: "delete"; table: string; row: Uint8Array; updatedAtMs?: number };

export type ThinBody =
  | { type: "ok" }
  | { type: "rows"; rows: Uint8Array; relation: boolean }
  | { type: "event"; delta: Uint8Array; reset: boolean; settled: boolean; tier: string }
  | { type: "closed" }
  | { type: "written"; row: Uint8Array; seq: number }
  | { type: "error"; code: string; message: string };

export interface ThinResponse {
  id: number;
  body: ThinBody;
}

const OP_INDEX: Record<ThinOp["type"], number> = {
  schema: 0,
  subscribe: 1,
  unsubscribe: 2,
  query: 3,
  insert: 4,
  update: 5,
  upsert: 6,
  delete: 7,
};

export function encodeThinRequest(id: number, op: ThinOp): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(WIRE_FRAME_THIN_REQUEST);
  writer.u64(id);
  writer.u64(OP_INDEX[op.type]);
  switch (op.type) {
    case "schema":
      writer.bytes(op.schema);
      break;
    case "subscribe":
    case "query":
      writer.bytes(op.query);
      writer.bool(op.includeDeleted);
      writer.u64(op.tier === "remote" ? 1 : 0);
      break;
    case "unsubscribe":
      writer.u64(op.subscription);
      break;
    case "insert":
      writer.string(op.table);
      if (op.row) {
        const row = op.row;
        writer.some((inner) => inner.bytes(rowId(row), false));
      } else {
        writer.none();
      }
      writer.bytes(op.cells);
      updatedAt(writer, op.updatedAtMs);
      break;
    case "update":
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      writer.bytes(op.patch);
      updatedAt(writer, op.updatedAtMs);
      break;
    case "upsert":
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      writer.bytes(op.cells);
      updatedAt(writer, op.updatedAtMs);
      break;
    case "delete":
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      updatedAt(writer, op.updatedAtMs);
      break;
  }
  return writer.finish();
}

/** Whether `frame` is a `WireFrame::ThinResponse`, by its postcard tag. */
export function isThinResponse(frame: Uint8Array): boolean {
  return frame[0] === WIRE_FRAME_THIN_RESPONSE;
}

export function decodeThinResponse(frame: Uint8Array): ThinResponse {
  const reader = new PostcardReader(frame);
  const tag = reader.u64();
  if (tag !== WIRE_FRAME_THIN_RESPONSE) {
    throw new Error(`expected WireFrame::ThinResponse, got tag ${tag}`);
  }
  const id = reader.u64();
  const variant = reader.u64();
  let body: ThinBody;
  switch (variant) {
    case 0:
      body = { type: "ok" };
      break;
    case 1:
      body = { type: "rows", rows: reader.bytes(), relation: reader.bool() };
      break;
    case 2:
      body = {
        type: "event",
        delta: reader.bytes(),
        reset: reader.bool(),
        settled: reader.bool(),
        tier: reader.string(),
      };
      break;
    case 3:
      body = { type: "closed" };
      break;
    case 4:
      body = { type: "written", row: reader.bytes(false), seq: reader.u64() };
      break;
    case 5:
      body = { type: "error", code: reader.string(), message: reader.string() };
      break;
    default:
      throw new Error(`unknown thin response body ${variant}`);
  }
  if (!reader.done()) throw new Error("trailing bytes after thin response");
  return { id, body };
}

function rowId(row: Uint8Array): Uint8Array {
  if (row.byteLength !== 16) throw new Error("a row id is 16 bytes");
  return row;
}

function updatedAt(writer: PostcardWriter, updatedAtMs: number | undefined): void {
  if (updatedAtMs === undefined) writer.none();
  else writer.some((inner) => inner.u64(updatedAtMs));
}
