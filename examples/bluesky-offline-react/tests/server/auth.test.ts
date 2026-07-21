import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  resolve: vi.fn(),
  invalidate: vi.fn(),
  restore: vi.fn(),
  terminalError: new Error("OAuth session expired"),
}));

vi.mock("@atproto/oauth-client-node", () => ({
  buildAtprotoLoopbackClientMetadata: (metadata: unknown) => metadata,
  isExpectedSessionError: (error: unknown) => error === mocks.terminalError,
  NodeOAuthClient: class NodeOAuthClient {
    restore = mocks.restore;
  },
}));

vi.mock("jose", () => ({
  importJWK: vi.fn().mockResolvedValue("private-key"),
  SignJWT: class SignJWT {
    setProtectedHeader() { return this; }
    setIssuer() { return this; }
    setIssuedAt() { return this; }
    setExpirationTime() { return this; }
    sign() { return Promise.resolve("signed-token"); }
  },
}));

vi.mock("../../server/jazz.js", () => ({
  authenticationDb: {},
}));

vi.mock("../../server/oauth-session-store.js", () => ({
  createBffSessionStore: () => ({
    create: vi.fn(),
    invalidate: mocks.invalidate,
    resolve: mocks.resolve,
  }),
  createEncryptedValueStore: () => ({}),
  createOAuthSessionStore: () => ({}),
  createOAuthStateStore: () => ({}),
}));

vi.mock("../../server/signing-keys.js", () => ({
  jazzJwt: {
    algorithm: "ES256",
    issuer: "bluesky-offline-react",
    keyId: "local-dev",
  },
  loadOrCreateJazzSigningKeys: vi.fn().mockResolvedValue({
    privateJwk: {},
    publicJwk: {},
  }),
}));

import { restoreBffSession } from "../../server/auth.js";

describe("BFF session restoration", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.resolve.mockResolvedValue("did:plc:alice");
    mocks.restore.mockResolvedValue({ fetchHandler: vi.fn() });
  });

  it("keeps the BFF session when ATProto restoration fails transiently", async () => {
    mocks.restore.mockRejectedValue(new Error("PDS unavailable"));

    expect(await restoreBffSession("opaque-session-id")).toBeNull();
    expect(mocks.invalidate).not.toHaveBeenCalled();
  });

  it("invalidates the BFF session when its ATProto credentials are terminally invalid", async () => {
    mocks.restore.mockRejectedValue(mocks.terminalError);

    expect(await restoreBffSession("opaque-session-id")).toBeNull();
    expect(mocks.invalidate).toHaveBeenCalledWith("opaque-session-id");
  });
});
