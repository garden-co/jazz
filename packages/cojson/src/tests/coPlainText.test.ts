import { afterEach, expect, test, vi } from "vitest";
import { expectPlainText } from "../coValue.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { nodeWithRandomAgentAndSessionID } from "./testUtils.js";

const Crypto = await WasmCrypto.create();

afterEach(() => void vi.unstubAllGlobals());

test("Empty CoPlainText works", () => {
  const node = nodeWithRandomAgentAndSessionID();

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
  const node = nodeWithRandomAgentAndSessionID();

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
  const node = nodeWithRandomAgentAndSessionID();

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

  content.insertAfter(4, " world", "trusting");
  expect(content.toString()).toEqual("hello world");

  content.insertBefore(0, "Hello, ", "trusting");
  expect(content.toString()).toEqual("Hello, hello world");

  content.deleteRange({ from: 6, to: 12 }, "trusting");
  expect(content.toString()).toEqual("Hello, world");

  content.insertBefore(2, "😍", "trusting");
  expect(content.toString()).toEqual("He😍llo, world");

  content.deleteRange({ from: 2, to: 3 }, "trusting");
  expect(content.toString()).toEqual("Hello, world");
});

test("Multiple items inserted appear in correct order", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  // Add multiple items in sequence
  content.insertAfter(0, "h", "trusting");
  content.insertAfter(0, "e", "trusting");
  content.insertAfter(1, "y", "trusting");

  // They should appear in insertion order (hey), not reversed (yeh)
  expect(content.toString()).toEqual("hey");
});

test("Items inserted at start appear with latest first", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  // Insert multiple items at the start
  content.insertAfter(0, "first", "trusting");
  content.insertBefore(0, "second", "trusting");
  content.insertBefore(0, "third", "trusting");

  // They should appear in reverse chronological order
  // because newer items should appear before older items
  expect(content.toString()).toEqual("thirdsecondfirst");
});

test("Handles different locales correctly", () => {
  const node = nodeWithRandomAgentAndSessionID();

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

test("insertBefore and insertAfter work as expected", () => {
  const node = nodeWithRandomAgentAndSessionID();
  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  // Insert 'h' at start
  content.insertBefore(0, "h", "trusting"); // "h"
  expect(content.toString()).toEqual("h");

  // Insert 'e' after 'h'
  content.insertAfter(0, "e", "trusting"); // "he"
  expect(content.toString()).toEqual("he");

  // Insert 'y' after 'e'
  content.insertAfter(1, "y", "trusting"); // "hey"
  expect(content.toString()).toEqual("hey");

  // Insert '!' at start
  content.insertBefore(0, "!", "trusting"); // "!hey"
  expect(content.toString()).toEqual("!hey");
});

test("Can delete a single grapheme", () => {
  const node = nodeWithRandomAgentAndSessionID();
  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });
  const content = expectPlainText(coValue.getCurrentContent());

  content.insertAfter(0, "a̐éö̲", "trusting"); // 3 graphemes
  content.deleteRange({ from: 1, to: 2 }, "trusting"); // delete the second grapheme
  expect(content.toString()).toEqual("a̐ö̲");
});

test("Handles complex grapheme clusters correctly", () => {
  const node = nodeWithRandomAgentAndSessionID();
  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });
  const content = expectPlainText(coValue.getCurrentContent());

  // Combining marks (should be treated as one grapheme each)
  const combining = "a̐éö̲"; // 3 graphemes: [a̐][é][ö̲]
  content.insertAfter(0, combining, "trusting");
  expect(content.toString()).toEqual(combining);
  content.deleteRange({ from: 1, to: 2 }, "trusting");
  expect(content.toString()).toEqual("a̐ö̲");

  // ZWJ emoji (family)
  const family = "👨‍👩‍👧‍👦"; // 1 grapheme
  content.insertAfter(2, family, "trusting");
  expect(content.toString()).toEqual("a̐ö̲👨‍👩‍👧‍👦");
  content.deleteRange({ from: 2, to: 3 }, "trusting");
  expect(content.toString()).toEqual("a̐ö̲");

  // Flag emoji (regional indicators)
  const flag = "🇺🇸"; // 1 grapheme
  content.insertAfter(2, flag, "trusting");
  expect(content.toString()).toEqual("a̐ö̲🇺🇸");
  content.deleteRange({ from: 2, to: 3 }, "trusting");
  expect(content.toString()).toEqual("a̐ö̲");

  // Emoji with skin tone modifier
  const thumbsUp = "👍🏽"; // 1 grapheme
  content.insertAfter(2, thumbsUp, "trusting");
  expect(content.toString()).toEqual("a̐ö̲👍🏽");
  content.deleteRange({ from: 2, to: 3 }, "trusting");
  expect(content.toString()).toEqual("a̐ö̲");
});

test("Handle deletion of complex grapheme clusters correctly", () => {
  const node = nodeWithRandomAgentAndSessionID();
  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });
  const content = expectPlainText(coValue.getCurrentContent());

  // Combining marks (should be treated as one grapheme each)
  content.insertAfter(0, "👋 안녕!", "trusting");
  expect(content.toString()).toEqual("👋 안녕!");

  // Delete the first grapheme
  content.deleteRange({ from: 0, to: 1 }, "trusting");
  expect(content.toString()).toEqual(" 안녕!");

  // Delete the second grapheme
  content.deleteRange({ from: 1, to: 2 }, "trusting");
  expect(content.toString()).toEqual(" 녕!");
});

test("Splits into and from grapheme string arrays", () => {
  const node = nodeWithRandomAgentAndSessionID();
  const coValue = node.createCoValue({
    type: "coplaintext",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectPlainText(coValue.getCurrentContent());

  content.insertAfter(0, "👋 안녕!", "trusting");
  expect(content.toString()).toEqual("👋 안녕!");

  const graphemes = content.toGraphemes("👋 안녕!");
  expect(graphemes).toEqual(["👋", " ", "안", "녕", "!"]);

  const text = content.fromGraphemes(graphemes);
  expect(text).toEqual("👋 안녕!");
});
