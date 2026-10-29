/** A frozen read frontier. Persist it with {@link serializeSnapshotRef}. */
export type SnapshotRef = string & { readonly __jazzSnapshotRef: unique symbol };

const PREFIX = "jazz-snapshot-v1:";
const HEADER_HEX = 72;
const DOT_HEX = 48;

/** An invalid reference or a snapshot format this SDK does not support. */
export class SnapshotRefFormatError extends Error {
  override readonly name = "SnapshotRefFormatError";
  readonly code = "invalid_snapshot_ref";
  constructor() {
    super("Invalid or unsupported snapshot reference");
  }
}

/**
 * Restore a reference from its documented string format. This validates the
 * encoding; it does not fetch history, retain data, or grant permission.
 */
export function parseSnapshotRef(value: unknown): SnapshotRef {
  if (typeof value !== "string" || !value.startsWith(PREFIX)) throw new SnapshotRefFormatError();
  const hex = value.slice(PREFIX.length);
  if (hex.length < HEADER_HEX || !/^[0-9a-f]+$/.test(hex)) throw new SnapshotRefFormatError();
  // Little-endian u32 dot count. Coordinates remain opaque bytes, so even the
  // largest u64 never passes through a JavaScript number.
  let count = 0;
  for (let byte = 0; byte < 4; byte++)
    count += parseInt(hex.slice(64 + byte * 2, 66 + byte * 2), 16) * 2 ** (byte * 8);
  if (hex.length !== HEADER_HEX + count * DOT_HEX) throw new SnapshotRefFormatError();
  return value as SnapshotRef;
}

/** Encode a captured or restored reference for an application row or storage. */
export function serializeSnapshotRef(reference: SnapshotRef): string {
  return parseSnapshotRef(reference);
}

/** The selected runtime cannot capture a frozen branch snapshot. */
export class UnsupportedSnapshotError extends Error {
  override readonly name = "UnsupportedSnapshotError";
  readonly code = "unsupported_snapshot";
  constructor(message = "Capturing snapshots is not supported by this runtime yet") {
    super(message);
  }
}
