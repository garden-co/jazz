import { expect, it } from "vitest";
import { createBrowserCrypto } from "./browser.js";
import { resolveCrypto } from "./crypto.js";
import type { LargeValueCipher } from "./types.js";

it("selects each override independently and retains omitted platform defaults", async () => {
  const defaults = await createBrowserCrypto();
  const customCell = { ...defaults.cellCipher };
  // Selection fixture only: deliberately not an encryption implementation.
  const large: LargeValueCipher = {
    mechanism: { id: "test.large", version: 1 },
    encrypt: (_key, _context, source) => source,
    decrypt: (_key, _context, source) => source,
  };
  const selected = await createBrowserCrypto({ cellCipher: customCell, largeValueCipher: large });
  expect(selected.cellCipher).toBe(customCell);
  expect(selected.keyEnvelope.mechanism).toEqual(defaults.keyEnvelope.mechanism);
  expect(selected.largeValueCipher).toBe(large);
  expect(defaults.largeValueCipher).toBeDefined();
});

it("does not initialise overridden defaults and rejects invalid mechanism versions", async () => {
  const adapters = await createBrowserCrypto();
  const unavailable = () => {
    throw new Error("default should not load");
  };
  const selected = await resolveCrypto(adapters, {
    cellCipher: unavailable,
    keyEnvelope: unavailable,
    deviceSigner: unavailable,
  });
  expect(selected.cellCipher).toBe(adapters.cellCipher);
  expect(selected.keyEnvelope).toBe(adapters.keyEnvelope);
  await expect(
    resolveCrypto(
      {
        ...adapters,
        cellCipher: {
          ...adapters.cellCipher,
          mechanism: { id: "test.cell", version: 0 },
        },
      },
      { cellCipher: unavailable, keyEnvelope: unavailable, deviceSigner: unavailable },
    ),
  ).rejects.toThrow();
});
