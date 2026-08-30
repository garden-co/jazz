// This repository-internal acceptance oracle deliberately reuses the exact
// production Jazz Tools decoder. It is not a second postcard implementation
// and does not add a low-level helper to the public React Native API.
import { PostcardReader, readNativeSubscriptionDelta } from "jazz-tools/_dev/native-binding-codec";

export function nativeSubscriptionDeltaHasRowId(
  payload: Uint8Array,
  expectedRowId: Uint8Array,
): boolean {
  if (expectedRowId.byteLength !== 16) return false;
  return nativeSubscriptionDeltaRowIds(payload).some((rowId) => sameBytes(rowId, expectedRowId));
}

export function nativeSubscriptionDeltaHasRows(payload: Uint8Array): boolean {
  return nativeSubscriptionDeltaRowIds(payload).length > 0;
}

export function nativeSubscriptionDeltaRowIds(payload: Uint8Array): Uint8Array[] {
  const delta = readNativeSubscriptionDelta(new PostcardReader(payload));
  return [...delta.added, ...delta.updated].flatMap((batch) => batch.rows.map((row) => row.rowId));
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index]);
}
