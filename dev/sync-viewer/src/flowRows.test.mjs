import assert from "node:assert/strict";
import test from "node:test";
import { buildFlowSql, flowPayloadDetails, resolveFlowAttrs } from "./flowRows.ts";

test("flow payload details include the payload attribute without payload json", () => {
  const attrs = resolveFlowAttrs({
    payload: "QuerySubscription",
    payload_json: "",
    fields: "",
  });

  assert.deepEqual(flowPayloadDetails(attrs), [
    { label: "payload", value: "QuerySubscription", kind: "text" },
  ]);
});

test("flow SQL returns only sync send and receive spans", () => {
  const sql = buildFlowSql({ minutes: 10, limit: 25, payloadFilter: "Row'Batch" });

  assert.match(sql, /SpanName IN \('sync\.send', 'sync\.recv'\)/);
  assert.match(sql, /SpanAttributes\['payload'\] = 'Row''Batch'/);
  assert.match(sql, /LIMIT 25/);
});
