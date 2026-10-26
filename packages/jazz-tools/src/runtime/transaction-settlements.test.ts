import { expect, it } from "vitest";
import type { RowSettlement } from "./client.js";
import { checkTransactionSettlements } from "./transaction-settlements.js";

const a: RowSettlement = { rowId: "alice-row", transactionId: "alice-content", position: "300" };
const b: RowSettlement = { rowId: "bob-row", transactionId: "bob-content", position: "301" };
const rows = [{ id: a.rowId }, { id: a.rowId }, { id: b.rowId }];

it("accepts an independent content receipt for each UNION occurrence", () => {
  expect(() => checkTransactionSettlements(rows, [a, b, { ...a }], true)).not.toThrow();
  expect(() => checkTransactionSettlements([rows[0]!, rows[2]!], [b, a])).not.toThrow();
  expect(() => checkTransactionSettlements([], [], true)).not.toThrow();
});

// Malformed binding data must not substitute one occurrence's authority proof
// for another or assign conflicting content history to the same root.
it.each([
  ["missing receipt", [a, b]],
  ["extra receipt", [a, a, b, b]],
  ["wrong occurrence count", [a, b, b]],
  ["foreign root", [a, a, { ...b, rowId: "mallory-row" }]],
  ["conflicting transaction", [a, { ...a, transactionId: "mallory-content" }, b]],
  ["conflicting position", [a, { ...a, position: "299" }, b]],
  ["negative position", [a, a, { ...b, position: "-1" }]],
  ["fractional position", [a, a, { ...b, position: "1.5" }]],
  ["overflowing position", [a, a, { ...b, position: "18446744073709551616" }]],
  ["numeric position", [a, a, { ...b, position: 301 }]],
  ["numeric transaction", [a, a, { ...b, transactionId: 301 }]],
] as const)("rejects %s in UNION settlements", (_name, entries) => {
  expect(() => checkTransactionSettlements(rows, [...entries] as RowSettlement[], true)).toThrow(
    "Incomplete E2EE authority settlement metadata",
  );
});

it("keeps duplicate ordinary rows and unexpected empty-result receipts invalid", () => {
  expect(() => checkTransactionSettlements(rows, [a, a, b])).toThrow();
  expect(() => checkTransactionSettlements([], [a], true)).toThrow();
});
