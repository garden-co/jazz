import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { parseSnapshotRef, serializeSnapshotRef, SnapshotRefFormatError } from "./snapshot-ref.js";

const fixture = JSON.parse(
  readFileSync(
    new URL("../../../../crates/jazz/fixtures/snapshot_ref_v1.json", import.meta.url),
    "utf8",
  ),
) as {
  cases: { name: string; reference: string }[];
  noncanonical: { name: string; reference: string; canonical: string }[];
  invalid: string[];
};

describe("application snapshot reference format", () => {
  for (const { name, reference } of fixture.cases) {
    it(`round-trips the Rust-owned ${name} fixture through JSON and storage`, () => {
      const restored = parseSnapshotRef(JSON.parse(JSON.stringify(reference)));
      expect(serializeSnapshotRef(restored)).toBe(reference);
    });
  }
  for (const { name, reference, canonical } of fixture.noncanonical) {
    it(`preserves opaque ${name} bytes and accepts Rust's canonical encoding`, () => {
      expect(serializeSnapshotRef(parseSnapshotRef(reference))).toBe(reference);
      expect(serializeSnapshotRef(parseSnapshotRef(canonical))).toBe(canonical);
    });
  }
  for (const reference of [...fixture.invalid, null, 1, {}, []]) {
    it(`rejects malformed or unsupported input ${JSON.stringify(reference)}`, () => {
      expect(() => parseSnapshotRef(reference)).toThrow(SnapshotRefFormatError);
    });
  }
});
