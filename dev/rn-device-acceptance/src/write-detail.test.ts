import assert from "node:assert/strict";
import test from "node:test";
import { publicClientWriteDetail } from "./write-detail.ts";

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
    publicClientWriteDetail(new TypeError("undefined is not a function (near '...x.y...')")),
    "public-client-write-detail:TypeError-none:undefined_is_not_a_function_near_...x.y..._",
  );
  const long = publicClientWriteDetail(new ReferenceError("a".repeat(200)));
  assert.equal(long, `public-client-write-detail:ReferenceError-none:${"a".repeat(96)}`);
});

test("an unnamed or odd thrown value stays an identifier", () => {
  assert.equal(publicClientWriteDetail(undefined), "public-client-write-detail:unknown-none");
  assert.equal(
    publicClientWriteDetail({ name: "has space" }),
    "public-client-write-detail:unknown-none",
  );
});
