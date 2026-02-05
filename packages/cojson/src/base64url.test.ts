import { expect, test } from "vitest";
import {
  base64URLtoBytes,
  bytesToBase64url,
  bytesToBase64,
} from "./base64url.js";

const txt = new TextEncoder();

test("Test our Base64 URL encoding and decoding", () => {
  // tests from the RFC

  expect(base64URLtoBytes("")).toEqual(new Uint8Array([]));
  expect(bytesToBase64url(new Uint8Array([]))).toEqual("");

  expect(bytesToBase64url(txt.encode("f"))).toEqual("Zg==");
  expect(bytesToBase64url(txt.encode("fo"))).toEqual("Zm8=");
  expect(bytesToBase64url(txt.encode("foo"))).toEqual("Zm9v");
  expect(bytesToBase64url(txt.encode("foob"))).toEqual("Zm9vYg==");
  expect(bytesToBase64url(txt.encode("fooba"))).toEqual("Zm9vYmE=");
  expect(bytesToBase64url(txt.encode("foobar"))).toEqual("Zm9vYmFy");
  // reverse
  expect(base64URLtoBytes("Zg==")).toEqual(txt.encode("f"));
  expect(base64URLtoBytes("Zm8=")).toEqual(txt.encode("fo"));
  expect(base64URLtoBytes("Zm9v")).toEqual(txt.encode("foo"));
  expect(base64URLtoBytes("Zm9vYg==")).toEqual(txt.encode("foob"));
  expect(base64URLtoBytes("Zm9vYmE=")).toEqual(txt.encode("fooba"));
  expect(base64URLtoBytes("Zm9vYmFy")).toEqual(txt.encode("foobar"));

  expect(base64URLtoBytes("V2hhdCBkb2VzIDIgKyAyLjEgZXF1YWw_PyB-IDQ=")).toEqual(
    txt.encode("What does 2 + 2.1 equal?? ~ 4"),
  );
  // reverse
  expect(bytesToBase64url(txt.encode("What does 2 + 2.1 equal?? ~ 4"))).toEqual(
    "V2hhdCBkb2VzIDIgKyAyLjEgZXF1YWw_PyB-IDQ=",
  );
});

test("Single special bytes: 0x00 and 0xFF", () => {
  const zero = new Uint8Array([0x00]);
  const encoded0 = bytesToBase64url(zero);
  expect(base64URLtoBytes(encoded0)).toEqual(zero);

  const ff = new Uint8Array([0xff]);
  const encodedFF = bytesToBase64url(ff);
  expect(base64URLtoBytes(encodedFF)).toEqual(ff);
});

test("All 256 byte values round-trip", () => {
  const allBytes = new Uint8Array(256);
  for (let i = 0; i < 256; i++) allBytes[i] = i;
  const encoded = bytesToBase64url(allBytes);
  expect(base64URLtoBytes(encoded)).toEqual(allBytes);
});

test("Decoding without padding", () => {
  expect(base64URLtoBytes("Zg")).toEqual(base64URLtoBytes("Zg=="));
  expect(base64URLtoBytes("Zm8")).toEqual(base64URLtoBytes("Zm8="));
  expect(base64URLtoBytes("Zm9vYg")).toEqual(base64URLtoBytes("Zm9vYg=="));
  expect(base64URLtoBytes("Zm9vYmE")).toEqual(base64URLtoBytes("Zm9vYmE="));
});

test("Larger binary data round-trip (4KB)", () => {
  const size = 4096;
  const data = new Uint8Array(size);
  // Deterministic pseudo-random fill
  let seed = 42;
  for (let i = 0; i < size; i++) {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    data[i] = seed & 0xff;
  }
  const encoded = bytesToBase64url(data);
  expect(base64URLtoBytes(encoded)).toEqual(data);
});

test("URL-safe characters: - and _ instead of + and /", () => {
  // 0xFB, 0xEF, 0xBE → standard base64 "++--", base64url "----"
  const bytes1 = new Uint8Array([0xfb, 0xef, 0xbe]);
  const encoded1 = bytesToBase64url(bytes1);
  expect(encoded1).not.toContain("+");
  expect(encoded1).not.toContain("/");
  expect(encoded1).toContain("-");
  expect(base64URLtoBytes(encoded1)).toEqual(bytes1);

  // 0xFF, 0xFF, 0xFE → standard base64 "///+", base64url "___-"
  const bytes2 = new Uint8Array([0xff, 0xff, 0xfe]);
  const encoded2 = bytesToBase64url(bytes2);
  expect(encoded2).not.toContain("+");
  expect(encoded2).not.toContain("/");
  expect(encoded2).toContain("_");
  expect(encoded2).toContain("-");
  expect(base64URLtoBytes(encoded2)).toEqual(bytes2);
});

test("bytesToBase64 produces standard base64 with + and /", () => {
  // 0xFB, 0xEF, 0xBE → standard base64 should contain "-" characters that become "+"
  const bytes1 = new Uint8Array([0xfb, 0xef, 0xbe]);
  const encoded1 = bytesToBase64(bytes1);
  expect(encoded1).not.toContain("-");
  expect(encoded1).not.toContain("_");
  expect(encoded1).toContain("+");

  // 0xFF, 0xFF, 0xFE → standard base64 "///+"
  const bytes2 = new Uint8Array([0xff, 0xff, 0xfe]);
  const encoded2 = bytesToBase64(bytes2);
  expect(encoded2).not.toContain("-");
  expect(encoded2).not.toContain("_");
  expect(encoded2).toContain("/");
  expect(encoded2).toContain("+");
  expect(encoded2).toEqual("///+");
});

test("bytesToBase64 RFC test vectors", () => {
  expect(bytesToBase64(new Uint8Array([]))).toEqual("");
  expect(bytesToBase64(txt.encode("f"))).toEqual("Zg==");
  expect(bytesToBase64(txt.encode("fo"))).toEqual("Zm8=");
  expect(bytesToBase64(txt.encode("foo"))).toEqual("Zm9v");
  expect(bytesToBase64(txt.encode("foob"))).toEqual("Zm9vYg==");
  expect(bytesToBase64(txt.encode("fooba"))).toEqual("Zm9vYmE=");
  expect(bytesToBase64(txt.encode("foobar"))).toEqual("Zm9vYmFy");
});
