import { expect, it } from "vitest";
import { createBrowserCrypto } from "./browser.js";
import { resolveCrypto } from "./crypto.js";

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
