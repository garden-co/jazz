import { Utf8Decoder } from "../utf8.js";
import { formatUuidAt } from "../hex.js";
import type { AcceptedRows, AcceptedRowEvidence, AcceptedVersion } from "../accepted-read.js";
import { PostcardReader, readNativeRowBatch, type NativeRowBatch } from "./native-codec.js";

const HEADER_LIMIT = 16 * 1024 * 1024;
const U64_MAX = 18446744073709551615n;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const TX_ID = /^[0-9a-f]{32}$/;
const decoder = new Utf8Decoder({ fatal: true });
function invalid(): never {
  throw new Error("Invalid jazz.accepted-read.host.v1 frame");
}
function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) invalid();
  return value as Record<string, unknown>;
}
function coordinate(value: unknown): string {
  if (typeof value !== "string" || !/^(0|[1-9][0-9]*)$/.test(value) || value.length > 20) invalid();
  if (BigInt(value) > U64_MAX) invalid();
  return value;
}
function uuid(value: unknown): string {
  if (typeof value !== "string" || !UUID.test(value)) invalid();
  return value;
}
function version(value: unknown): AcceptedVersion {
  const entry = object(value);
  if (typeof entry.transactionId !== "string" || !TX_ID.test(entry.transactionId)) invalid();
  return { transactionId: entry.transactionId, position: coordinate(entry.position) };
}

// The existing row decoder accepts a reader interface. Grow vectors only as
// their items decode, so a small malformed carrier cannot preallocate from an
// untrusted length. Every item in the row codec consumes at least one byte.
class AcceptedRowReader extends PostcardReader {
  override readVec<T>(readItem: (reader: PostcardReader) => T): T[] {
    const length = this.u64();
    const items: T[] = [];
    for (let i = 0; i < length; i += 1) items.push(readItem(this));
    return items;
  }
}

/** Decode the named in-process envelope and its ordinary row batches together.
 * Pairing is validated before typed transforms can consume any row evidence. */
export function decodeAcceptedRead(frame: Uint8Array): Omit<AcceptedRows<never>, "rows"> & {
  batches: NativeRowBatch[];
} {
  if (frame.byteLength < 5) invalid();
  const size = new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(0, true);
  if (size === 0 || size > HEADER_LIMIT || size > frame.byteLength - 5) invalid();
  const header = object(JSON.parse(decoder.decode(frame.subarray(4, 4 + size))));
  if (header.format !== "jazz.accepted-read.host.v1") invalid();
  const through = coordinate(header.through);
  const schema = uuid(header.schema);
  if (!Array.isArray(header.evidence)) invalid();
  const ids = new Set<string>();
  const evidence: AcceptedRowEvidence[] = header.evidence.map((value) => {
    const entry = object(value);
    const rowId = uuid(entry.rowId);
    const state = version(entry.state);
    const content = version(entry.content);
    if (
      ids.has(rowId) ||
      BigInt(content.position) === 0n ||
      BigInt(content.position) > BigInt(state.position) ||
      BigInt(state.position) > BigInt(through)
    )
      invalid();
    ids.add(rowId);
    return { rowId, state, content };
  });
  const reader = new AcceptedRowReader(frame.subarray(4 + size));
  // A contiguous batch contains at least one row in the producer's codec.
  // Check before allocating the outer vector from a malformed length.
  const batchCount = reader.u64();
  if (batchCount > evidence.length) invalid();
  const batches = Array.from({ length: batchCount }, () => readNativeRowBatch(reader));
  if (!reader.done()) invalid();
  let index = 0;
  for (const batch of batches) {
    if (batch.rows.length === 0) invalid();
    for (const row of batch.rows) {
      if (row.rowId.length !== 16 || formatUuidAt(row.rowId, 0) !== evidence[index]?.rowId)
        invalid();
      index += 1;
    }
  }
  if (index !== evidence.length) invalid();
  return { through, schema, evidence, batches };
}
