import { expect, it } from "vitest";
import { createBrowserCellCipher } from "./browser.js";

it("encrypts cells with fresh nonces and rejects another authenticated context", async () => {
  const cipher = await createBrowserCellCipher();
  const key = new Uint8Array(32).fill(7);
  const context = new Uint8Array([1, 2, 3]);
  const plaintext = new TextEncoder().encode("private value");
  const first = await cipher.encrypt(key, context, plaintext);
  const second = await cipher.encrypt(key, context, plaintext);
  expect(first).not.toEqual(second);
  expect(await cipher.decrypt(key, context, first)).toEqual(plaintext);
  await expect(cipher.decrypt(key, new Uint8Array([1, 2, 4]), first)).rejects.toThrow();
  const corrupted = first.slice();
  corrupted[corrupted.length - 1]! ^= 1;
  await expect(cipher.decrypt(key, context, corrupted)).rejects.toThrow();
});
