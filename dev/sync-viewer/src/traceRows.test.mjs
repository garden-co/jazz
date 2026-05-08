import assert from "node:assert/strict";
import test from "node:test";
import { traceRowPayloadLabel } from "./traceRows.ts";

test("trace waterfall rows show the promoted payload attribute", () => {
  assert.equal(
    traceRowPayloadLabel({
      payload: "QuerySubscription",
      "jazz.span.fields": JSON.stringify({ payload: "BatchFate" }),
    }),
    "QuerySubscription",
  );
});

test("trace waterfall rows fall back to payload inside legacy fields", () => {
  assert.equal(
    traceRowPayloadLabel({
      "jazz.span.fields": JSON.stringify({ payload: "RowBatchCreated" }),
    }),
    "RowBatchCreated",
  );
});
