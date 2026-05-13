import assert from "node:assert/strict";
import test from "node:test";
import { QUERY_ENDPOINT_ENV, runTelemetryQuery } from "./vite.config.ts";

test("runTelemetryQuery posts SQL to the configured endpoint", async () => {
  const previousFetch = globalThis.fetch;
  const previousEndpoint = process.env[QUERY_ENDPOINT_ENV];
  const calls = [];
  process.env[QUERY_ENDPOINT_ENV] = "http://127.0.0.1:4318/query";
  globalThis.fetch = async (url, init) => {
    calls.push({ url, init });
    return new Response('{"ok":true}\n', { status: 200 });
  };

  try {
    const output = await runTelemetryQuery("SELECT 1");

    assert.equal(output, '{"ok":true}\n');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, "http://127.0.0.1:4318/query");
    assert.equal(calls[0].init.method, "POST");
    assert.equal(calls[0].init.headers["content-type"], "application/json");
    assert.equal(calls[0].init.body, JSON.stringify({ sql: "SELECT 1" }));
  } finally {
    globalThis.fetch = previousFetch;
    if (previousEndpoint === undefined) {
      delete process.env[QUERY_ENDPOINT_ENV];
    } else {
      process.env[QUERY_ENDPOINT_ENV] = previousEndpoint;
    }
  }
});
