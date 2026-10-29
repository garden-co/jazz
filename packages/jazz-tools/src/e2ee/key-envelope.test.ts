import { expect, it } from "vitest";
import { createBrowserKeyEnvelope } from "./browser.js";

it("wraps a key with fresh nonces and authenticates its context", async () => {
  const adapter = await createBrowserKeyEnvelope();
  const wrappingKey = new Uint8Array(32).fill(7);
  const key = new Uint8Array(32).fill(9);
  const context = new Uint8Array([1, 2, 3]);
  const first = await adapter.wrap(wrappingKey, context, key);
  expect(await adapter.wrap(wrappingKey, context, key)).not.toEqual(first);
  expect(await adapter.unwrap(wrappingKey, context, first)).toEqual(key);
  await expect(adapter.unwrap(wrappingKey, new Uint8Array([1, 2, 4]), first)).rejects.toThrow();
  await expect(adapter.wrap(wrappingKey, context, new Uint8Array(31))).rejects.toThrow();
  const corrupted = first.slice();
  corrupted[corrupted.length - 1]! ^= 1;
  await expect(adapter.unwrap(wrappingKey, context, corrupted)).rejects.toThrow();
});

it("seals keys to one device and verifies context inside the sealed plaintext", async () => {
  const adapter = await createBrowserKeyEnvelope();
  const device = await adapter.createKeyPair();
  const other = await adapter.createKeyPair();
  const key = new Uint8Array(32).fill(9);
  const context = new Uint8Array([1, 2, 3]);
  const sealed = await adapter.seal(device.publicKey, context, key);
  expect(await adapter.seal(device.publicKey, context, key)).not.toEqual(sealed);
  expect(await adapter.open(device, context, sealed)).toEqual(key);
  await expect(adapter.open(other, context, sealed)).rejects.toThrow();
  await expect(adapter.open(device, new Uint8Array([1, 2, 4]), sealed)).rejects.toThrow();
  await expect(adapter.unwrap(key, context, sealed)).rejects.toThrow();
  const corrupted = sealed.slice();
  corrupted[corrupted.length - 1]! ^= 1;
  await expect(adapter.open(device, context, corrupted)).rejects.toThrow();
});
