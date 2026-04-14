import { describe, it, expect } from "vitest";
import { PasskeyBackupError } from "./passkey-backup.js";

describe("PasskeyBackupError", () => {
  it("has the correct name", () => {
    const err = new PasskeyBackupError("not-supported");
    expect(err.name).toBe("PasskeyBackupError");
  });

  it("exposes the code", () => {
    const err = new PasskeyBackupError("invalid-secret");
    expect(err.code).toBe("invalid-secret");
  });

  it("uses the default message for each code", () => {
    expect(new PasskeyBackupError("not-supported").message).toBe(
      "WebAuthn is not supported in this browser",
    );
    expect(new PasskeyBackupError("invalid-secret").message).toBe(
      "Secret must be a 32-byte base64url string",
    );
    expect(new PasskeyBackupError("create-failed").message).toBe(
      "Failed to create passkey credential",
    );
    expect(new PasskeyBackupError("get-failed").message).toBe(
      "Failed to retrieve passkey credential",
    );
    expect(new PasskeyBackupError("no-credential").message).toBe("No passkey credential found");
    expect(new PasskeyBackupError("invalid-credential").message).toBe(
      "Passkey credential does not contain a valid secret",
    );
  });

  it("attaches cause when provided", () => {
    const cause = new Error("underlying");
    const err = new PasskeyBackupError("create-failed", cause);
    expect(err.cause).toBe(cause);
  });

  it("is an instance of Error", () => {
    expect(new PasskeyBackupError("not-supported")).toBeInstanceOf(Error);
  });
});
