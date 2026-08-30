import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import {
  nativeSubscriptionDeltaHasRowId,
  nativeSubscriptionDeltaHasRows,
  nativeSubscriptionDeltaRowIds,
} from "./native-subscription-observation.ts";

const fixture = JSON.parse(
  fs.readFileSync(
    path.resolve(import.meta.dirname, "../../../crates/jazz/fixtures/binding_codec_golden.json"),
    "utf8",
  ),
) as { subscription_deltas: Array<{ name: string; payload_hex: string }> };
const encoded = Uint8Array.from(
  Buffer.from(
    fixture.subscription_deltas.find(
      ({ name }) => name === "added_updated_removed_with_v1_and_v2_occurrence_keys",
    )!.payload_hex,
    "hex",
  ),
);

test("subscription observation inspects structured added and updated row identities", () => {
  assert.equal(nativeSubscriptionDeltaHasRowId(encoded, new Uint8Array(16).fill(0x11)), true);
  assert.equal(nativeSubscriptionDeltaHasRowId(encoded, new Uint8Array(16).fill(0x21)), true);

  // 0xb1 appears in the real postcard payload's occurrence-key sidecar. A
  // raw-byte scan would accept it even though it is not an added/updated row.
  assert.equal(nativeSubscriptionDeltaHasRowId(encoded, new Uint8Array(16).fill(0xb1)), false);
  // Removed identities also do not prove a newly visible row.
  assert.equal(nativeSubscriptionDeltaHasRowId(encoded, new Uint8Array(16).fill(0x13)), false);
  assert.equal(nativeSubscriptionDeltaHasRows(encoded), true);
  assert.deepEqual(
    nativeSubscriptionDeltaRowIds(encoded).map((rowId) => rowId[0]),
    [0x11, 0x21],
  );
  assert.throws(
    () => nativeSubscriptionDeltaRowIds(encoded.subarray(0, encoded.length - 1)),
    /postcard|subscription|unexpected/i,
  );
});
