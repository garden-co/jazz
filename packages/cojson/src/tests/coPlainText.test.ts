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
    "Intl.Segmenter is not supported. Use a polyfill to get coPlainText support in Jazz. (eg. https://formatjs.github.io/docs/polyfills/intl-segmenter/)",
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

  content.insertAfter(0, "Hello, ", "trusting");
  expect(content.toString()).toEqual("Hello, hello world");

  console.log("first delete");
  content.deleteRange({ from: 6, to: 12 }, "trusting");
  expect(content.toString()).toEqual("Hello, world");

  content.insertAfter(2, "😍", "trusting");
  expect(content.toString()).toEqual("He😍llo, world");

  console.log("second delete");
  content.deleteRange({ from: 2, to: 4 }, "trusting");
  expect(content.toString()).toEqual("Hello, world");
});

test("Multiple items appended after start appear in correct order", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  // Add multiple items in a single transaction, all after start
  content.insertAfter(0, "h", "trusting");
  content.insertAfter(1, "e", "trusting");
  content.insertAfter(2, "y", "trusting");

  // They should appear in insertion order (hey), not reversed (yeh)
  expect(content.toString()).toEqual("hey");
});

test("Items inserted at start appear with latest first", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  // Insert multiple items at the start
  content.insertAfter(0, "first", "trusting");
  content.insertAfter(0, "second", "trusting");
  content.insertAfter(0, "third", "trusting");

  // They should appear in reverse chronological order
  // because newer items should appear before older items
  expect(content.toString()).toEqual("thirdsecondfirst");
});

test("Handles different locales correctly", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  // Test with explicit locale in meta
  const coValueJa = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: { locale: "ja-JP" },
    ...Crypto.createdNowUnique(),
  });

  const contentJa = expectPlainText(coValueJa.getCurrentContent());
  contentJa.insertAfter(0, "こんにちは", "trusting");
  expect(contentJa.toString()).toEqual("こんにちは");

  // Test browser locale fallback
  vi.stubGlobal("navigator", { language: "fr-FR" });

  const coValueBrowser = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const contentBrowser = expectPlainText(coValueBrowser.getCurrentContent());
  contentBrowser.insertAfter(0, "bonjour", "trusting");
  expect(contentBrowser.toString()).toEqual("bonjour");

  // Test fallback to 'en' when no navigator
  vi.stubGlobal("navigator", undefined);

  const coValueFallback = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const contentFallback = expectPlainText(coValueFallback.getCurrentContent());
  contentFallback.insertAfter(0, "hello", "trusting");
  expect(contentFallback.toString()).toEqual("hello");
});
