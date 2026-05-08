import assert from "node:assert/strict";
import test from "node:test";
import { flowPayloadDetails, resolveFlowAttrs } from "./flowRows.ts";

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
