import { afterEach, expect, test, vi } from "vitest";
import { expectPlainText } from "../coValue.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { LocalNode } from "../localNode.js";
import { randomAnonymousAccountAndSessionID } from "./testUtils.js";

const Crypto = await WasmCrypto.create();

afterEach(() => void vi.unstubAllGlobals());

test("should throw on creation if Intl.Segmenter is not available", () => {
  vi.stubGlobal("Intl", {
    Segmenter: undefined,
  });

  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);
  const group = node.createGroup();
  expect(() => group.createPlainText()).toThrow(
    "Intl.Segmenter is not supported for this compilation target. Use a polyfill to get coPlainText support in Jazz. (eg. https://formatjs.github.io/docs/polyfills/intl-segmenter/)",
  );
});

test("Empty CoPlainText works", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  expect(content.type).toEqual("coplaintext");
  expect(content.toString()).toEqual("");
});

test("Can insert into empty CoPlainText", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  expect(content.type).toEqual("coplaintext");

  content.insertAfter(0, "a", "trusting");
  expect(content.toString()).toEqual("a");
});

test("Can insert and delete in CoPlainText", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  expect(content.type).toEqual("coplaintext");

  content.insertAfter(0, "hello", "trusting");
  expect(content.toString()).toEqual("hello");

  content.insertAfter(5, " world", "trusting");
  expect(content.toString()).toEqual("hello world");

  console.log("first delete");
  content.deleteRange({ from: 3, to: 8 }, "trusting");
  expect(content.toString()).toEqual("helrld");

  content.insertAfter(2, "😍", "trusting");
  expect(content.toString()).toEqual("he😍lrld");

  console.log("second delete");
  content.deleteRange({ from: 2, to: 4 }, "trusting");
  expect(content.toString()).toEqual("helrld");
});
