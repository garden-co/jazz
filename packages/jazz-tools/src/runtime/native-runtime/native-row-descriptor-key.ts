import { PostcardWriter, type NativeRowBatch } from "./native-codec.js";
import { writeNativeRowDescriptor, writeValueType, type ValueType } from "./native-row-codec.js";

const byteHex = Array.from({ length: 256 }, (_, byte) => byte.toString(16).padStart(2, "0"));

/**
 * A wire-exact key for a native ValueType.
 *
 * ValueType is recursive and several tags carry semantic data beyond their
 * numeric tag (record fields, enum registry ids, and payload enum cases). The
 * native wire codec is the canonical representation of that data, so using it
 * here keeps cache identity aligned with the protocol rather than maintaining
 * a second partial structural encoder.
 */
export function valueTypeCacheKey(type: ValueType): string {
  const writer = new PostcardWriter();
  writeValueType(writer, type);
  return bytesToHex(writer.finish());
}

/** A wire-exact cache key for the descriptor that controls row decoding. */
export function nativeRowFieldPlanCacheKey(
  batch: Pick<NativeRowBatch, "table" | "descriptor">,
): string {
  const writer = new PostcardWriter();
  writer.string(batch.table);
  writeNativeRowDescriptor(writer, [...batch.descriptor]);
  return bytesToHex(writer.finish());
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byteHex[byte]).join("");
}
