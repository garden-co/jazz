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

/**
 * Options are the JSON objects the native bindings take; transactions are
 * named by their open transaction id text.
 */
export type ThinOp =
  | { type: "schema"; schema: Uint8Array }
  | { type: "subscribe"; query: Uint8Array; options: object }
  | { type: "unsubscribe"; subscription: number }
  | { type: "query"; query: Uint8Array; options: object; transaction?: string }
  | { type: "insert"; table: string; cells: Uint8Array; options: object; transaction?: string }
  | {
      type: "update";
      table: string;
      row: Uint8Array;
      patch: Uint8Array;
      options: object;
      transaction?: string;
    }
  | {
      type: "upsert";
      table: string;
      row: Uint8Array;
      cells: Uint8Array;
      options: object;
      transaction?: string;
    }
  | { type: "delete"; table: string; row: Uint8Array; options: object; transaction?: string }
  | {
      type: "restore";
      table: string;
      row: Uint8Array;
      cells?: Uint8Array;
      options: object;
      transaction?: string;
    }
  | {
      type: "updateLargeValues";
      table: string;
      row: Uint8Array;
      patch: Uint8Array;
      mutations: readonly unknown[];
      updatedAtMs?: number;
      transaction?: string;
    }
  | { type: "begin"; transaction: string; kind: string }
  | { type: "commit"; transaction: string }
  | { type: "rollback"; transaction: string }
  | { type: "captureSnapshot" }
  | {
      type: "beginStream";
      table: string;
      row: Uint8Array;
      cells: Uint8Array;
      column: string;
      mutation: string;
      options: object;
    }
  | { type: "streamChunk"; stream: number; chunk: Uint8Array }
  | { type: "finishStream"; stream: number }
  | { type: "abortStream"; stream: number };

export type ThinBody =
  | { type: "ok" }
  | { type: "rows"; rows: Uint8Array; relation: boolean }
  | { type: "event"; delta: Uint8Array; reset: boolean; settled: boolean; tier: string }
  | { type: "closed" }
  | { type: "staged"; row: Uint8Array }
  | { type: "snapshot"; snapshot: Uint8Array }
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
  restore: 8,
  updateLargeValues: 9,
  begin: 10,
  commit: 11,
  rollback: 12,
  captureSnapshot: 13,
  beginStream: 14,
  streamChunk: 15,
  finishStream: 16,
  abortStream: 17,
};

const encoder = new TextEncoder();

/** JSON with byte arrays as arrays of numbers, as the native bindings read them. */
function json(value: unknown): Uint8Array {
  return encoder.encode(
    JSON.stringify(value, (_key, inner: unknown) =>
      inner instanceof Uint8Array ? Array.from(inner) : inner,
    ),
  );
}

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
      writer.bytes(op.query);
      writer.bytes(json(op.options));
      break;
    case "unsubscribe":
      writer.u64(op.subscription);
      break;
    case "query":
      writer.bytes(op.query);
      writer.bytes(json(op.options));
      transaction(writer, op.transaction);
      break;
    case "insert":
      writer.string(op.table);
      writer.bytes(op.cells);
      writer.bytes(json(op.options));
      transaction(writer, op.transaction);
      break;
    case "update":
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      writer.bytes(op.patch);
      writer.bytes(json(op.options));
      transaction(writer, op.transaction);
      break;
    case "upsert":
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      writer.bytes(op.cells);
      writer.bytes(json(op.options));
      transaction(writer, op.transaction);
      break;
    case "delete":
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      writer.bytes(json(op.options));
      transaction(writer, op.transaction);
      break;
    case "restore": {
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      const cells = op.cells;
      if (cells) writer.some((inner) => inner.bytes(cells));
      else writer.none();
      writer.bytes(json(op.options));
      transaction(writer, op.transaction);
      break;
    }
    case "updateLargeValues": {
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      writer.bytes(op.patch);
      writer.bytes(json(op.mutations));
      const updatedAtMs = op.updatedAtMs;
      if (updatedAtMs === undefined) writer.none();
      else writer.some((inner) => inner.u64(updatedAtMs));
      transaction(writer, op.transaction);
      break;
    }
    case "begin":
      writer.string(op.transaction);
      writer.string(op.kind);
      break;
    case "commit":
    case "rollback":
      writer.string(op.transaction);
      break;
    case "captureSnapshot":
      break;
    case "beginStream":
      writer.string(op.table);
      writer.bytes(rowId(op.row), false);
      writer.bytes(op.cells);
      writer.string(op.column);
      writer.string(op.mutation);
      writer.bytes(json(op.options));
      break;
    case "streamChunk":
      writer.u64(op.stream);
      writer.bytes(op.chunk);
      break;
    case "finishStream":
    case "abortStream":
      writer.u64(op.stream);
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
      body = { type: "staged", row: reader.bytes(false) };
      break;
    case 5:
      body = { type: "snapshot", snapshot: reader.bytes() };
      break;
    case 6:
      body = { type: "written", row: reader.bytes(false), seq: reader.u64() };
      break;
    case 7:
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

function transaction(writer: PostcardWriter, id: string | undefined): void {
  if (id === undefined) writer.none();
  else writer.some((inner) => inner.string(id));
}
