import { inspect } from "node:util";
import { describe, expect, it } from "vitest";
import { EncryptionNotDeclaredError, notDeclaredEncryption } from "./index.js";

describe("db.encryption of an app that declares no encryption", () => {
  it("throws on use, naming why", () => {
    const encryption = notDeclaredEncryption();
    expect(() => encryption.devices).toThrow(EncryptionNotDeclaredError);
    expect(() => encryption.ready()).toThrow(EncryptionNotDeclaredError);
  });

  it("can be logged, inspected, serialised and awaited", async () => {
    const encryption = notDeclaredEncryption();
    expect(() => inspect(encryption)).not.toThrow();
    expect(() => inspect({ encryption }, { showProxy: false, customInspect: true })).not.toThrow();
    // `console.log` formats with `util.inspect`; `String()` reads `Symbol.toStringTag`.
    expect(Object.prototype.toString.call(encryption)).toBe("[object Object]");
    expect(JSON.stringify({ encryption })).toBe('{"encryption":{}}');
    // Not a thenable: awaiting it answers the handle itself.
    expect(await encryption).toBe(encryption);
  });
});
