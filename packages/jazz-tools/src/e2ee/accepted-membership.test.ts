import { expect, it } from "vitest";
import type { AcceptedRows } from "../runtime/accepted-read.js";
import { acceptedContentPositions } from "./accepted-membership.js";

// Consumer ordering fixtures, not a substitute for real authority admission.
function restored(): AcceptedRows<{ id: string }> {
  return {
    rows: [{ id: "root" }],
    schema: "fixture",
    through: "18446744073709551615",
    evidence: [
      {
        rowId: "root",
        state: { transactionId: "restore", position: "9007199254741010" },
        content: { transactionId: "original-root", position: "9007199254740999" },
      },
    ],
  };
}

it("keeps the signed content's exact activation order when its state is restored later", () => {
  expect(acceptedContentPositions(restored()).get("root")).toBe(9007199254740999n);
});

it.each(["missing", "extra", "duplicate"])(
  "refuses %s per-row evidence instead of inventing complete membership",
  (mode) => {
    const snapshot = restored();
    if (mode === "missing") snapshot.evidence = [];
    if (mode === "extra") snapshot.evidence[0]!.rowId = "another-account";
    if (mode === "duplicate") snapshot.rows.push({ id: "root" });
    expect(() => acceptedContentPositions(snapshot)).toThrow(/coverage/);
  },
);

it.each(["future-content", "past-cut", "overflow"])(
  "refuses an invalid %s authority coordinate",
  (mode) => {
    const snapshot = restored();
    if (mode === "future-content") snapshot.evidence[0]!.content.position = "9007199254741011";
    if (mode === "past-cut") snapshot.through = "9007199254741009";
    if (mode === "overflow") snapshot.evidence[0]!.state.position = "18446744073709551616";
    expect(() => acceptedContentPositions(snapshot)).toThrow(/authority/);
  },
);
