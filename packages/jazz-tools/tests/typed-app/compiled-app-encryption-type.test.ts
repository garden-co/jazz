// The `encryption` object of a generated file's compiled schema takes the
// settings the generator writes (Rust `EncryptionSettings`), and only them.
import { describe, expect, it } from "vitest";
import type { CompiledAppSchema } from "../../src/typed-app.js";

type Encryption = NonNullable<CompiledAppSchema["encryption"]>;

describe("CompiledAppSchema encryption", () => {
  it("accepts the settings a generated file carries", () => {
    const generated: Encryption = {
      settings: { allowAccountReset: false },
      versions: {},
    };
    expect(generated.settings?.allowAccountReset).toBe(false);
  });

  it("rejects a misspelt settings key", () => {
    const misspelt: Encryption = {
      // @ts-expect-error `allowAccountReset` is misspelt
      settings: { allowAccountReset: false, allowAcountReset: true },
    };
    expect(misspelt).toBeDefined();
  });
});
