import { expect, it } from "vitest";
import { encodeCryptoContext } from "./context.js";

it("pins field order and length framing without normalising identity strings", () => {
  const context = { application: "a", policy: "p", scope: "s", identifier: "i", epoch: "e" };
  expect(encodeCryptoContext(context)).toEqual(
    new Uint8Array([
      74, 69, 50, 67, 1, 0, 0, 0, 1, 97, 0, 0, 0, 1, 112, 0, 0, 0, 1, 115, 0, 0, 0, 1, 105, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 101, 0, 0, 0, 0,
    ]),
  );
  expect(encodeCryptoContext({ ...context, application: "ab", policy: "c" })).not.toEqual(
    encodeCryptoContext({ ...context, application: "a", policy: "bc" }),
  );
  expect(() => encodeCryptoContext({ ...context, application: "\ud800" })).toThrow();
});

it("preserves a leading U+FEFF as identity data rather than interpreting it as a BOM", () => {
  const encoded = encodeCryptoContext({
    application: "\ufeffa",
    policy: "p",
    scope: "s",
    identifier: "i",
    epoch: "e",
  });
  expect(encoded.subarray(0, 13)).toEqual(
    new Uint8Array([74, 69, 50, 67, 1, 0, 0, 0, 4, 239, 187, 191, 97]),
  );
});
