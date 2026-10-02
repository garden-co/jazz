import { expect, it } from "vitest";
import { createBrowserCellCipher } from "./browser.js";
import { encodeCryptoContext } from "./context.js";
import { createNativeCellCipher } from "./native.js";

it("exchanges authenticated cells between native Rust and browser crypto", async () => {
  const browser = await createBrowserCellCipher();
  const native = await createNativeCellCipher();
  const key = new Uint8Array(32).fill(7);
  const context = encodeCryptoContext({
    application: "app",
    policy: "policy",
    scope: "projects",
    identifier: "project",
    epoch: "1",
    table: "todos",
    row: "row",
    column: "title",
  });
  const plaintext = new TextEncoder().encode("private value");
  const browserCell = await browser.encrypt(key, context, plaintext);
  expect(await native.decrypt(key, context, browserCell)).toEqual(plaintext);
  const nativeCell = await native.encrypt(key, context, plaintext);
  expect(await browser.decrypt(key, context, nativeCell)).toEqual(plaintext);
  const corrupted = browserCell.slice();
  corrupted[corrupted.length - 1]! ^= 1;
  await expect(native.decrypt(key, context, corrupted)).rejects.toThrow();
  await expect(native.decrypt(new Uint8Array(31), context, browserCell)).rejects.toThrow();
});
