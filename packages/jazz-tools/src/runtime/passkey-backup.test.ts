import { describe, it, expect, vi, afterEach } from "vitest";
import { PasskeyBackupError, BrowserPasskeyBackup } from "./passkey-backup.js";

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

describe("BrowserPasskeyBackup.backup — not-supported", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("throws not-supported when navigator.credentials is absent", async () => {
    vi.stubGlobal("navigator", {});
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.backup("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")).rejects.toMatchObject({
      code: "not-supported",
    });
  });

  it("throws not-supported when navigator is undefined", async () => {
    vi.stubGlobal("navigator", undefined);
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.backup("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")).rejects.toMatchObject({
      code: "not-supported",
    });
  });
});

describe("BrowserPasskeyBackup.backup — invalid-secret", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("throws invalid-secret when the secret is not valid base64url", async () => {
    vi.stubGlobal("navigator", { credentials: { create: vi.fn() } });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.backup("not!!!valid!!!base64url")).rejects.toMatchObject({
      code: "invalid-secret",
    });
  });

  it("throws invalid-secret when the secret decodes to fewer than 32 bytes", async () => {
    vi.stubGlobal("navigator", { credentials: { create: vi.fn() } });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    // 16 zero bytes as base64url
    await expect(pb.backup("AAAAAAAAAAAAAAAAAAAAAA")).rejects.toMatchObject({
      code: "invalid-secret",
    });
  });

  it("throws invalid-secret when the secret decodes to more than 32 bytes", async () => {
    vi.stubGlobal("navigator", { credentials: { create: vi.fn() } });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    // 33 zero bytes as base64url
    const bytes = new Uint8Array(33);
    let bin = "";
    for (const b of bytes) bin += String.fromCharCode(b);
    const tooLong = btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    await expect(pb.backup(tooLong)).rejects.toMatchObject({ code: "invalid-secret" });
  });
});

const VALID_SECRET = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"; // 32 zero bytes

describe("BrowserPasskeyBackup.backup — credentials.create", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("calls credentials.create with user.id equal to the decoded secret bytes", async () => {
    const mockCreate = vi.fn().mockResolvedValue({});
    vi.stubGlobal("navigator", { credentials: { create: mockCreate } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await pb.backup(VALID_SECRET);

    expect(mockCreate).toHaveBeenCalledOnce();
    const callArg = mockCreate.mock.calls[0][0] as {
      publicKey: PublicKeyCredentialCreationOptions;
    };
    const userId = new Uint8Array(callArg.publicKey.user.id as ArrayBuffer);
    expect(userId).toEqual(new Uint8Array(32)); // 32 zero bytes
  });

  it("sets residentKey: required on authenticatorSelection", async () => {
    const mockCreate = vi.fn().mockResolvedValue({});
    vi.stubGlobal("navigator", { credentials: { create: mockCreate } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await pb.backup(VALID_SECRET);

    const callArg = mockCreate.mock.calls[0][0] as {
      publicKey: PublicKeyCredentialCreationOptions;
    };
    expect(callArg.publicKey.authenticatorSelection?.residentKey).toBe("required");
    expect(callArg.publicKey.authenticatorSelection?.requireResidentKey).toBe(true);
  });

  it("sets rp.id to appHostname", async () => {
    const mockCreate = vi.fn().mockResolvedValue({});
    vi.stubGlobal("navigator", { credentials: { create: mockCreate } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "myapp.com" });
    await pb.backup(VALID_SECRET);

    const callArg = mockCreate.mock.calls[0][0] as {
      publicKey: PublicKeyCredentialCreationOptions;
    };
    expect(callArg.publicKey.rp.id).toBe("myapp.com");
  });

  it("sets pubKeyCredParams with ES256 first then RS256", async () => {
    const mockCreate = vi.fn().mockResolvedValue({});
    vi.stubGlobal("navigator", { credentials: { create: mockCreate } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await pb.backup(VALID_SECRET);

    const callArg = mockCreate.mock.calls[0][0] as {
      publicKey: PublicKeyCredentialCreationOptions;
    };
    expect(callArg.publicKey.pubKeyCredParams).toEqual([
      { alg: -7, type: "public-key" },
      { alg: -257, type: "public-key" },
    ]);
  });

  it("throws create-failed with cause when credentials.create rejects", async () => {
    const underlying = new Error("User cancelled");
    const mockCreate = vi.fn().mockRejectedValue(underlying);
    vi.stubGlobal("navigator", { credentials: { create: mockCreate } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.backup(VALID_SECRET)).rejects.toMatchObject({
      code: "create-failed",
      cause: underlying,
    });
  });
});
