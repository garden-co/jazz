import assert from "node:assert/strict";
import test from "node:test";
import { decodeBase64, encodeBase64 } from "./base64.ts";

test("native relay base64 codec does not rely on browser globals", () => {
  const originalAtob = globalThis.atob;
  const originalBtoa = globalThis.btoa;
  try {
    // Plant the exact release-Hermes environment which exposed the failure.
    Object.defineProperty(globalThis, "atob", { configurable: true, value: undefined });
    Object.defineProperty(globalThis, "btoa", { configurable: true, value: undefined });
    const bytes = Uint8Array.of(0, 1, 2, 3, 254, 255);
    assert.equal(encodeBase64(bytes), "AAECA/7/");
    assert.deepEqual(decodeBase64("AAECA/7/"), bytes);
  } finally {
    Object.defineProperty(globalThis, "atob", { configurable: true, value: originalAtob });
    Object.defineProperty(globalThis, "btoa", { configurable: true, value: originalBtoa });
  }
});

test("native relay base64 codec rejects malformed opaque data", () => {
  assert.throws(() => decodeBase64("not-base64"), /malformed base64/);
  assert.deepEqual(decodeBase64("AA=="), Uint8Array.of(0));
});
