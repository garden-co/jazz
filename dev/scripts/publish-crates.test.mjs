import assert from "node:assert/strict";
import { test } from "node:test";
import { maxWaitMinutes, rateLimitRetryAt } from "./publish-crates.mjs";

const now = new Date("2026-11-17T00:02:00Z");

test("a 429 with a time of day retries at that time today", () => {
  const output =
    "error: failed to publish to registry at https://crates.io\n\nCaused by:\n  the remote server responded with an error (status 429 Too Many Requests): You have published too many new crates in a short period of time. Please try again after 00:06:08 GMT or email help@crates.io to have your limit increased.";
  assert.equal(rateLimitRetryAt(output, now).toISOString(), "2026-11-17T00:06:08.000Z");
});

test("a 429 with a full date retries at that date", () => {
  const output =
    "status 429 Too Many Requests: Please try again after Tue, 17 Nov 2026 00:16:08 GMT";
  assert.equal(rateLimitRetryAt(output, now).toISOString(), "2026-11-17T00:16:08.000Z");
});

test("a time of day already past means tomorrow", () => {
  const output = "429 Too Many Requests: Please try again after 00:01:00 GMT";
  assert.equal(rateLimitRetryAt(output, now).toISOString(), "2026-11-18T00:01:00.000Z");
});

test("a 429 without a time waits ten minutes", () => {
  assert.equal(
    rateLimitRetryAt("status 429 Too Many Requests", now).toISOString(),
    "2026-11-17T00:12:00.000Z",
  );
});

test("other failures are not retried", () => {
  assert.equal(rateLimitRetryAt("error: crate version `2.0.0` is already uploaded", now), null);
});

test("a full date on another day is used as is, not as a time of day", () => {
  const output =
    "the remote server responded with an error (status 429 Too Many Requests): Please try again after Wed, 18 Nov 2026 09:30:00 GMT";
  assert.equal(rateLimitRetryAt(output, now).toISOString(), "2026-11-18T09:30:00.000Z");
});

test("a 429 elsewhere in cargo's output is not a rate limit", () => {
  const output =
    "   Packaged 12 files, 429.1KiB (98.0KiB compressed)\nerror: failed to publish: the remote server responded with an error (status 500 Internal Server Error)";
  assert.equal(rateLimitRetryAt(output, now), null);
});

test("the wait cap defaults when unset or empty, and refuses anything else", () => {
  assert.equal(maxWaitMinutes(undefined), 180);
  assert.equal(maxWaitMinutes(""), 180);
  assert.equal(maxWaitMinutes(" 45 "), 45);
  assert.equal(maxWaitMinutes("0"), 0);
  assert.throws(() => maxWaitMinutes("3h"), /whole number of minutes/);
  assert.throws(() => maxWaitMinutes("-5"), /whole number of minutes/);
});
