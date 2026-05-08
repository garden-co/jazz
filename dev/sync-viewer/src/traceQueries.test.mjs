import assert from "node:assert/strict";
import test from "node:test";
import { buildTraceListSql } from "./traceQueries.ts";

test("trace list search can narrow results by trace id text", () => {
  const sql = buildTraceListSql({
    minutes: 30,
    serviceFilter: "",
    opFilter: "",
    traceIdFilter: "9f7A",
  });

  assert.match(sql, /TraceId ILIKE '%9f7A%'/);
});
