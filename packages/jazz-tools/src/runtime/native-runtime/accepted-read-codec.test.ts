import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { decodeAcceptedRead } from "./accepted-read-codec.js";
import { validateAcceptedReadTimeout } from "../accepted-read.js";

const fixtures = JSON.parse(
  readFileSync(
    new URL("../../../../../crates/jazz/fixtures/accepted_read_host_v1.json", import.meta.url),
    "utf8",
  ),
).cases as Array<{
  name: string;
  header: Record<string, any>;
  frame_hex: string;
  rows_hex: string;
}>;
const bytes = (hex: string) => Uint8Array.from(Buffer.from(hex, "hex"));
function frame(header: unknown, rows: Uint8Array): Uint8Array {
  const json = new TextEncoder().encode(JSON.stringify(header));
  const result = new Uint8Array(4 + json.length + rows.length);
  new DataView(result.buffer).setUint32(0, json.length, true);
  result.set(json, 4);
  result.set(rows, 4 + json.length);
  return result;
}
const paired = fixtures[1]!;
const rows = bytes(paired.rows_hex);
function changed(change: (header: Record<string, any>) => void): Uint8Array {
  const header = structuredClone(paired.header);
  change(header);
  return frame(header, rows);
}

describe("jazz.accepted-read.host.v1 independent fixtures", () => {
  it.each(fixtures)("decodes $name without rounding authority coordinates", (fixture) => {
    const decoded = decodeAcceptedRead(bytes(fixture.frame_hex));
    expect(decoded).toMatchObject({
      through: fixture.header.through,
      schema: fixture.header.schema,
      evidence: fixture.header.evidence,
    });
    expect(decoded.batches.flatMap((batch) => batch.rows)).toHaveLength(
      fixture.header.evidence.length,
    );
    if (fixture.name === "paired_u64") {
      expect(decoded.through).toBe("18446744073709551615");
      expect(decoded.evidence[0]?.content.position).toBe("9007199254740993");
      expect(decoded.evidence[0]?.state).not.toEqual(decoded.evidence[0]?.content);
      expect(decoded.batches[0]?.rows[0]?.deleted).toBe(true);
    }
  });
  it.each([
    [
      "version",
      (h: any) => {
        h.format = "jazz.accepted-read.host.v2";
      },
    ],
    [
      "schema UUID",
      (h: any) => {
        h.schema = "not-a-uuid";
      },
    ],
    [
      "row UUID",
      (h: any) => {
        h.evidence[0].rowId = "11";
      },
    ],
    [
      "transaction id",
      (h: any) => {
        h.evidence[0].content.transactionId = "ff";
      },
    ],
    [
      "number coordinate",
      (h: any) => {
        h.through = 1;
      },
    ],
    [
      "u64 overflow",
      (h: any) => {
        h.through = "18446744073709551616";
      },
    ],
    [
      "noncanonical coordinate",
      (h: any) => {
        h.through = "01";
      },
    ],
    [
      "zero content",
      (h: any) => {
        h.evidence[0].content.position = "0";
      },
    ],
    [
      "content after state",
      (h: any) => {
        h.evidence[0].state.position = "1";
      },
    ],
    [
      "state after cut",
      (h: any) => {
        h.through = "1";
      },
    ],
    [
      "duplicate row",
      (h: any) => {
        h.evidence.push(h.evidence[0]);
      },
    ],
    [
      "missing row evidence",
      (h: any) => {
        h.evidence = [];
      },
    ],
    [
      "row pairing",
      (h: any) => {
        h.evidence[0].rowId = h.schema;
      },
    ],
    [
      "extra evidence",
      (h: any) => {
        const other = structuredClone(h.evidence[0]);
        other.rowId = h.schema;
        h.evidence.push(other);
      },
    ],
  ] as const)("rejects malformed %s", (_name, change) => {
    expect(() => decodeAcceptedRead(changed(change))).toThrow();
  });
  it("validates row ordering as well as the count", () => {
    const header = structuredClone(paired.header);
    const other = structuredClone(header.evidence[0]);
    other.rowId = header.schema;
    header.evidence.push(other);
    // Independently assemble two rows in one existing postcard batch.
    const twoRows = bytes(
      "01056e6f7465730002" + "10" + "11".repeat(16) + "0100" + "10" + "22".repeat(16) + "0100",
    );
    expect(decodeAcceptedRead(frame(header, twoRows)).evidence).toEqual(header.evidence);
    header.evidence.reverse();
    expect(() => decodeAcceptedRead(frame(header, twoRows))).toThrow();
  });
  it("rejects truncation, trailing bytes, and unbounded vector declarations", () => {
    const full = bytes(paired.frame_hex);
    expect(() => decodeAcceptedRead(full.subarray(0, 3))).toThrow();
    expect(() => decodeAcceptedRead(full.subarray(0, full.length - 1))).toThrow();
    expect(() => decodeAcceptedRead(Uint8Array.from([...full, 0]))).toThrow();
    expect(() => decodeAcceptedRead(frame(paired.header, bytes("ffffffff07")))).toThrow();
    // Same huge declaration inside the existing row decoder's descriptor.
    expect(() => decodeAcceptedRead(frame(paired.header, bytes("0100ffffffff07")))).toThrow();
    expect(() => decodeAcceptedRead(frame(paired.header, bytes("010000ffffffff07")))).toThrow();
  });
  it("bounds and validates the header before parsing it", () => {
    const full = bytes(paired.frame_hex);
    for (const size of [0, full.length, 16 * 1024 * 1024 + 1]) {
      const invalid = full.slice();
      new DataView(invalid.buffer).setUint32(0, size, true);
      expect(() => decodeAcceptedRead(invalid)).toThrow();
    }
    const badUtf8 = full.slice();
    badUtf8[4] = 0xff;
    expect(() => decodeAcceptedRead(badUtf8)).toThrow();
    expect(() => decodeAcceptedRead(frame(null, new Uint8Array([0])))).toThrow();
  });
  it("matches the raw host safe-integer timeout contract", () => {
    for (const value of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])
      expect(() => validateAcceptedReadTimeout(value)).toThrow();
    for (const value of [0, 1, Number.MAX_SAFE_INTEGER])
      expect(() => validateAcceptedReadTimeout(value)).not.toThrow();
  });
});
