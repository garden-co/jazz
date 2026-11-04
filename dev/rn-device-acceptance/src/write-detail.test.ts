import assert from "node:assert/strict";
import test from "node:test";
import { publicClientWriteDetail } from "./write-detail.ts";

const withoutStack = <E extends Error>(error: E): E => Object.assign(error, { stack: "" });

test("a Jazz write error reports its class and code, never its message", () => {
  const error = Object.assign(new Error("row title secret"), {
    name: "PersistedWriteRejectedError",
    code: "permission_denied",
  });
  assert.equal(
    publicClientWriteDetail(error),
    "public-client-write-detail:PersistedWriteRejectedError-permission_denied",
  );
});

test("an engine TypeError carries its message squeezed to identifier characters", () => {
  assert.equal(
    publicClientWriteDetail(
      withoutStack(new TypeError("undefined is not a function (near '...x.y...')")),
    ),
    "public-client-write-detail:TypeError-none:undefined_is_not_a_function_near_...x.y..._",
  );
  const long = publicClientWriteDetail(withoutStack(new ReferenceError("a".repeat(200))));
  assert.equal(long, `public-client-write-detail:ReferenceError-none:${"a".repeat(96)}`);
});

test("an unnamed or odd thrown value stays an identifier", () => {
  assert.equal(publicClientWriteDetail(undefined), "public-client-write-detail:unknown-none");
  assert.equal(
    publicClientWriteDetail({ name: "has space" }),
    "public-client-write-detail:unknown-none",
  );
});

test("an engine TypeError names the functions it was thrown in", () => {
  const hermes = new TypeError("undefined is not a function");
  hermes.stack =
    "TypeError: undefined is not a function\n    at insertInternal (address at index.android.bundle:1:2)\n    at insert (address at index.android.bundle:1:3)\n    at anonymous (address at x:1:4)\n    at deeper (x)";
  assert.equal(
    publicClientWriteDetail(hermes),
    "public-client-write-detail:TypeError-none:undefined_is_not_a_function_at_insertInternal_at_insert_at_anonymous",
  );
  const jsc = new TypeError("undefined is not a function");
  jsc.stack =
    "insertInternal@http://localhost/index.bundle:1:2\ninsert@http://localhost/index.bundle:1:3";
  assert.equal(
    publicClientWriteDetail(jsc),
    "public-client-write-detail:TypeError-none:undefined_is_not_a_function_at_insertInternal_at_insert",
  );
});
