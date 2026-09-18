import { expect, it } from "vitest";
import { decodeAcceptedHistoryReads, encodeAcceptedHistoryReads } from "./accepted-history.js";

// Durable local format corpus: query text is opaque here, never executed.
const corpus =
  '[{"query":"fixture","snapshot":{"rows":[{"id":"row","payload":{"e2eeBytesV1":[0,127,255]}}],"settlements":[{"rowId":"row","transactionId":"transaction","position":"18446744073709551615"}]}}]';

it("pins the v1 public-history encoding including bytes and exact u64 positions", () => {
  const reads = [
    {
      query: "fixture",
      snapshot: {
        rows: [{ id: "row", payload: new Uint8Array([0, 127, 255]) }],
        settlements: [
          { rowId: "row", transactionId: "transaction", position: "18446744073709551615" },
        ],
      },
    },
  ];
  expect(encodeAcceptedHistoryReads(reads)).toBe(corpus);
  expect(decodeAcceptedHistoryReads(corpus)).toEqual(reads);
});

it.each([
  "null",
  "[]",
  corpus.replace("[0,127,255]", "[0,127,256]"),
  corpus.replace("[0,127,255]", '[0,"127",255]'),
  corpus.replace("18446744073709551615", "18446744073709551616"),
  corpus.replace("18446744073709551615", "0001"),
  corpus.replace("18446744073709551615", "9".repeat(100)),
  corpus.replace('"e2eeBytesV1":', '"extra":true,"e2eeBytesV1":'),
])("rejects malformed stored history: %s", (encoded) => {
  expect(() => decodeAcceptedHistoryReads(encoded)).toThrow();
});
