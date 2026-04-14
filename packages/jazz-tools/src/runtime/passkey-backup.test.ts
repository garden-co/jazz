import { describe, it, expect, vi, afterEach } from "vitest";
import { PasskeyBackupError, BrowserPasskeyBackup } from "./passkey-backup.js";

// Build a minimal authenticatorData buffer. The flags byte lives at offset 32.
// UP = bit 0 (0x01), UV = bit 2 (0x04). Both set = 0x05.
function makeAuthData(flags: number): ArrayBuffer {
  const buf = new Uint8Array(37); // 32 (rpIdHash) + 1 (flags) + 4 (signCount)
  buf[32] = flags;
  return buf.buffer;
}

const UP_UV = 0x05; // user present + user verified

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
    expect(new PasskeyBackupError("verification-failed").message).toBe(
      "Authenticator did not perform user verification",
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

  it("sets authenticatorAttachment: platform", async () => {
    const mockCreate = vi.fn().mockResolvedValue({});
    vi.stubGlobal("navigator", { credentials: { create: mockCreate } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await pb.backup(VALID_SECRET);

    const callArg = mockCreate.mock.calls[0][0] as {
      publicKey: PublicKeyCredentialCreationOptions;
    };
    expect(callArg.publicKey.authenticatorSelection?.authenticatorAttachment).toBe("platform");
  });

  it("sets userVerification: required on authenticatorSelection", async () => {
    const mockCreate = vi.fn().mockResolvedValue({});
    vi.stubGlobal("navigator", { credentials: { create: mockCreate } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await pb.backup(VALID_SECRET);

    const callArg = mockCreate.mock.calls[0][0] as {
      publicKey: PublicKeyCredentialCreationOptions;
    };
    expect(callArg.publicKey.authenticatorSelection?.userVerification).toBe("required");
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

  it("sets credentialProtectionPolicy: userVerificationRequired extension", async () => {
    const mockCreate = vi.fn().mockResolvedValue({});
    vi.stubGlobal("navigator", { credentials: { create: mockCreate } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await pb.backup(VALID_SECRET);

    const callArg = mockCreate.mock.calls[0][0] as {
      publicKey: PublicKeyCredentialCreationOptions & {
        extensions?: Record<string, unknown>;
      };
    };
    expect(callArg.publicKey.extensions?.credentialProtectionPolicy).toBe(
      "userVerificationRequired",
    );
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

describe("BrowserPasskeyBackup.restore — not-supported", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("throws not-supported when navigator.credentials is absent", async () => {
    vi.stubGlobal("navigator", {});
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.restore()).rejects.toMatchObject({ code: "not-supported" });
  });
});

describe("BrowserPasskeyBackup.restore — get-failed", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("throws get-failed with cause when credentials.get rejects", async () => {
    const underlying = new Error("User cancelled");
    vi.stubGlobal("navigator", {
      credentials: { get: vi.fn().mockRejectedValue(underlying) },
    });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.restore()).rejects.toMatchObject({
      code: "get-failed",
      cause: underlying,
    });
  });
});

describe("BrowserPasskeyBackup.restore — no-credential", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("throws no-credential when credentials.get returns null", async () => {
    vi.stubGlobal("navigator", {
      credentials: { get: vi.fn().mockResolvedValue(null) },
    });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.restore()).rejects.toMatchObject({ code: "no-credential" });
  });
});

describe("BrowserPasskeyBackup.restore — verification-failed", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("throws verification-failed when neither UP nor UV is set", async () => {
    const mockCredential = {
      type: "public-key",
      response: {
        userHandle: new Uint8Array(32).buffer,
        authenticatorData: makeAuthData(0x00),
      },
    };
    vi.stubGlobal("navigator", {
      credentials: { get: vi.fn().mockResolvedValue(mockCredential) },
    });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.restore()).rejects.toMatchObject({ code: "verification-failed" });
  });

  it("throws verification-failed when UV is set but UP is not", async () => {
    const mockCredential = {
      type: "public-key",
      response: {
        userHandle: new Uint8Array(32).buffer,
        authenticatorData: makeAuthData(0x04), // UV only
      },
    };
    vi.stubGlobal("navigator", {
      credentials: { get: vi.fn().mockResolvedValue(mockCredential) },
    });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.restore()).rejects.toMatchObject({ code: "verification-failed" });
  });

  it("throws verification-failed when UP is set but UV is not", async () => {
    const mockCredential = {
      type: "public-key",
      response: {
        userHandle: new Uint8Array(32).buffer,
        authenticatorData: makeAuthData(0x01), // UP only
      },
    };
    vi.stubGlobal("navigator", {
      credentials: { get: vi.fn().mockResolvedValue(mockCredential) },
    });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.restore()).rejects.toMatchObject({ code: "verification-failed" });
  });
});

describe("BrowserPasskeyBackup.restore — invalid-credential", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("throws invalid-credential when userHandle is null", async () => {
    const mockCredential = {
      type: "public-key",
      response: {
        userHandle: null,
        authenticatorData: makeAuthData(UP_UV),
      },
    };
    vi.stubGlobal("navigator", {
      credentials: { get: vi.fn().mockResolvedValue(mockCredential) },
    });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.restore()).rejects.toMatchObject({ code: "invalid-credential" });
  });

  it("throws invalid-credential when userHandle is not 32 bytes", async () => {
    const mockCredential = {
      type: "public-key",
      response: {
        userHandle: new Uint8Array(16).buffer,
        authenticatorData: makeAuthData(UP_UV),
      },
    };
    vi.stubGlobal("navigator", {
      credentials: { get: vi.fn().mockResolvedValue(mockCredential) },
    });
    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await expect(pb.restore()).rejects.toMatchObject({ code: "invalid-credential" });
  });
});

describe("BrowserPasskeyBackup.restore — happy path", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns the secret encoded in userHandle as base64url", async () => {
    const secretBytes = new Uint8Array(32).fill(0);
    const mockCredential = {
      type: "public-key",
      response: {
        userHandle: secretBytes.buffer,
        authenticatorData: makeAuthData(UP_UV),
      },
    };
    vi.stubGlobal("navigator", {
      credentials: { get: vi.fn().mockResolvedValue(mockCredential) },
    });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    const secret = await pb.restore();
    // 32 zero bytes → base64url
    expect(secret).toBe("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
  });

  it("passes rpId and userVerification: required to credentials.get", async () => {
    const mockGet = vi.fn().mockResolvedValue({
      type: "public-key",
      response: {
        userHandle: new Uint8Array(32).buffer,
        authenticatorData: makeAuthData(UP_UV),
      },
    });
    vi.stubGlobal("navigator", { credentials: { get: mockGet } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "myapp.com" });
    await pb.restore();

    const callArg = mockGet.mock.calls[0][0] as CredentialRequestOptions;
    expect(callArg.publicKey?.rpId).toBe("myapp.com");
    expect(callArg.publicKey?.userVerification).toBe("required");
    expect(callArg.mediation).toBe("required");
  });
});

describe("BrowserPasskeyBackup round-trip", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("backup secret bytes match restored secret", async () => {
    // Capture the user.id bytes passed to credentials.create
    let capturedUserId: Uint8Array | null = null;
    const mockCreate = vi
      .fn()
      .mockImplementation((opts: { publicKey: PublicKeyCredentialCreationOptions }) => {
        capturedUserId = new Uint8Array(opts.publicKey.user.id as ArrayBuffer);
        return Promise.resolve({});
      });

    // Generate a random 32-byte secret
    const rawBytes = new Uint8Array(32);
    crypto.getRandomValues(rawBytes);
    let bin = "";
    for (const b of rawBytes) bin += String.fromCharCode(b);
    const originalSecret = btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

    vi.stubGlobal("navigator", { credentials: { create: mockCreate, get: vi.fn() } });

    const pb = new BrowserPasskeyBackup({ appName: "Test App", appHostname: "test.example" });
    await pb.backup(originalSecret);

    // Now restore using the captured bytes as the userHandle
    const mockGet = vi.fn().mockResolvedValue({
      type: "public-key",
      response: {
        userHandle: capturedUserId!.buffer,
        authenticatorData: makeAuthData(UP_UV),
      },
    });
    vi.stubGlobal("navigator", { credentials: { create: vi.fn(), get: mockGet } });

    const restoredSecret = await pb.restore();
    expect(restoredSecret).toBe(originalSecret);
  });
});
