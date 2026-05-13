import assert from "node:assert/strict";
import test from "node:test";
import { runQuery } from "./api.ts";

test("runQuery posts SQL to the local query endpoint", async () => {
  const previousFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, init) => {
    calls.push({ url, init });
    return new Response('{"answer":42}\n', { status: 200 });
  };

  try {
    const rows = await runQuery(" SELECT 42 ");

    assert.deepEqual(rows, [{ answer: 42 }]);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, "/api/query");
    assert.equal(calls[0].init.method, "POST");
    assert.equal(calls[0].init.headers["content-type"], "application/json");
    assert.equal(calls[0].init.body, JSON.stringify({ sql: "SELECT 42" }));
  } finally {
    globalThis.fetch = previousFetch;
  }
});
