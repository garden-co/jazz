import { randomUUID } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  isRequestAuthenticationError,
  RequestAuthenticationError,
  resolveRequestSession,
} from "./request-auth.js";

const mocks = vi.hoisted(() => ({
  verifyLocalFirstIdentityProof: vi.fn(),
}));

// The verifier itself is covered by local-first-token.test.ts and the
// src/local-first-auth vectors; these tests drive its outcome.
vi.mock("../runtime/local-first-token.js", () => ({
  verifyLocalFirstToken: (token: string, appId: string) => {
    const result = mocks.verifyLocalFirstIdentityProof(token, appId);
    if (!result?.ok) throw new Error("invalid local-first token");
    return result.id;
  },
}));

function base64Url(input: string): string {
  return Buffer.from(input, "utf8")
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function makeUnsignedJwt(payload: Record<string, unknown>): string {
  return `${base64Url(JSON.stringify({ alg: "none", typ: "JWT" }))}.${base64Url(JSON.stringify(payload))}.signature`;
}

const GUEST = "11111111-1111-1111-1111-111111111111";
const ACCOUNT = "00000000-0000-4000-8000-000000000012";
const guestToken = (sub = GUEST) => makeUnsignedJwt({ iss: "urn:jazz:local-first", sub });
const registryConfig = {
  appId: "app",
  accountRegistry: "https://core.example/apps/app/accounts",
  backendSecret: "backend-secret",
};

describe("backend request auth", () => {
  beforeEach(() => {
    mocks.verifyLocalFirstIdentityProof.mockReset();
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it("refuses an external JWT, naming createJazz's authenticate as who decides", async () => {
    const fetcher = vi.spyOn(globalThis, "fetch");
    const token = makeUnsignedJwt({ iss: "https://issuer.example", sub: "reader" });
    const result = resolveRequestSession(
      { headers: { authorization: `Bearer ${token}` } },
      registryConfig,
    );
    await expect(result).rejects.toBeInstanceOf(RequestAuthenticationError);
    await expect(result).rejects.toThrow(/createJazz.*authenticate/);
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("reads the bearer from a mixed-case plain-record Authorization key", async () => {
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: GUEST });
    await expect(
      resolveRequestSession({ headers: { Authorization: `Bearer ${guestToken()}` } }, {
        appId: "app",
      }),
    ).resolves.toMatchObject({ user_id: GUEST, authMode: "local-first" });
  });

  it("rejects conflicting case variants in plain-record authorization headers", async () => {
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: GUEST });
    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${guestToken()}`,
            Authorization: `Bearer ${guestToken(randomUUID())}`,
          },
        },
        { appId: "app" },
      ),
    ).rejects.toBeInstanceOf(RequestAuthenticationError);
  });

  it("reads the method-provided authorization before plain-record headers exactly once", async () => {
    mocks.verifyLocalFirstIdentityProof.mockImplementation((token: string) => ({
      ok: true,
      id: JSON.parse(Buffer.from(token.split(".")[1]!, "base64url").toString()).sub,
    }));
    const header = vi
      .fn()
      .mockReturnValueOnce(`Bearer ${guestToken()}`)
      .mockImplementation(() => {
        throw new Error("request headers read twice");
      });
    const session = await resolveRequestSession(
      { header, headers: { authorization: `Bearer ${guestToken(randomUUID())}` } },
      { appId: "app" },
    );
    expect(session.user_id).toBe(GUEST);
    expect(header).toHaveBeenCalledTimes(1);
  });

  it("rejects a missing, non-bearer or empty Authorization header, and a non-JWT", async () => {
    for (const authorization of [undefined, "Basic abc", "Bearer ", "Bearer not-a-jwt"]) {
      await expect(
        resolveRequestSession(
          { headers: authorization ? { authorization } : {} },
          { appId: "app" },
        ),
        String(authorization),
      ).rejects.toBeInstanceOf(RequestAuthenticationError);
    }
  });

  it("rejects an invalid local-first proof before asking the registry", async () => {
    const fetcher = vi.spyOn(globalThis, "fetch");
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: false });
    await expect(
      resolveRequestSession(
        { headers: { authorization: `Bearer ${guestToken()}` } },
        registryConfig,
      ),
    ).rejects.toThrow(/Invalid local-first/);
    expect(fetcher).not.toHaveBeenCalled();
  });

  it.each([
    ["identity_not_assigned", 404],
    ["identity_not_authorized", 403],
  ] as const)("rejects registry %s as the caller's failure", async (code, status) => {
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: GUEST });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(code, { status }));
    const result = resolveRequestSession(
      { headers: { authorization: `Bearer ${guestToken()}` } },
      registryConfig,
    );
    await expect(result).rejects.toBeInstanceOf(RequestAuthenticationError);
    await expect(result).rejects.toMatchObject({ code });
  });

  it("keeps a registry outage, or an answer without a rejection code, a server error", async () => {
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: GUEST });
    for (const response of [
      new Response("down", { status: 503 }),
      new Response("", { status: 403 }),
      new Response("not json"),
    ]) {
      vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(response);
      const result = resolveRequestSession(
        { headers: { authorization: `Bearer ${guestToken()}` } },
        registryConfig,
      );
      await expect(result).rejects.toThrow();
      await expect(result).rejects.not.toBeInstanceOf(RequestAuthenticationError);
    }
  });

  it("rejects malformed registry answers and an answer for another identity", async () => {
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: GUEST });
    for (const answer of [
      { account: "not-an-account", identity: { issuer: "urn:jazz:local-first", subject: GUEST } },
      { account: ACCOUNT, identity: { issuer: "urn:jazz:local-first", subject: randomUUID() } },
    ]) {
      vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(JSON.stringify(answer)));
      await expect(
        resolveRequestSession(
          { headers: { authorization: `Bearer ${guestToken()}` } },
          registryConfig,
        ),
      ).rejects.toMatchObject({ code: "invalid_account_response" });
    }
  });

  it("founds a verified local-first account under its proven identity", async () => {
    const identity = {
      issuer: "urn:jazz:local-first",
      subject: "11111111-1111-1111-1111-111111111111",
    };
    const token = makeUnsignedJwt({ iss: identity.issuer, sub: identity.subject });
    const account = "00000000-0000-4000-8000-000000000012";
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: identity.subject });
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response(JSON.stringify({ account, identity })));
    const result = await resolveRequestSession(
      { headers: { authorization: `Bearer ${token}` } },
      {
        appId: "app",
        accountRegistry: "https://core.example/apps/app/accounts",
        backendSecret: "backend-secret",
      },
    );
    expect(result.account_id).toBe(account);
    expect(fetcher).toHaveBeenCalledWith(
      "https://core.example/apps/app/admin/accounts/found-local-first",
      expect.objectContaining({
        headers: expect.objectContaining({ "X-Jazz-Backend-Secret": "backend-secret" }),
      }),
    );
  });

  it("accepts local-first JWTs without a registry and uses the shared session mapping", async () => {
    const fetcher = vi.spyOn(globalThis, "fetch");
    const appId = "local-first-backend-app";
    const userId = "11111111-1111-1111-1111-111111111111";
    const token = makeUnsignedJwt({
      iss: "urn:jazz:local-first",
      sub: userId,
      auth_mode: "local-first",
    });
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: userId });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId,
        },
      ),
    ).resolves.toEqual({
      issuer: "urn:jazz:local-first",
      user_id: userId,
      // Supported custom JWT fields are policy inputs for reserved issuers
      // too; authMode is derived separately from verified issuer admission.
      claims: { auth_mode: "local-first" },
      authMode: "local-first",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it.each([
    { nodeEnv: undefined, allowLocalFirstAuth: undefined, accepted: true },
    { nodeEnv: "development", allowLocalFirstAuth: undefined, accepted: true },
    { nodeEnv: "test", allowLocalFirstAuth: undefined, accepted: true },
    { nodeEnv: "production", allowLocalFirstAuth: undefined, accepted: false },
    { nodeEnv: "PrOdUcTiOn", allowLocalFirstAuth: undefined, accepted: false },
    { nodeEnv: "development", allowLocalFirstAuth: true, accepted: true },
    { nodeEnv: "development", allowLocalFirstAuth: false, accepted: false },
    { nodeEnv: "production", allowLocalFirstAuth: true, accepted: true },
    { nodeEnv: "production", allowLocalFirstAuth: false, accepted: false },
  ])(
    "local-first JWT accepted=$accepted when NODE_ENV=$nodeEnv and override=$allowLocalFirstAuth",
    async ({ nodeEnv, allowLocalFirstAuth, accepted }) => {
      vi.stubEnv("NODE_ENV", nodeEnv);
      const identity = {
        issuer: "urn:jazz:local-first",
        subject: "11111111-1111-1111-1111-111111111111",
      };
      const token = makeUnsignedJwt({ iss: identity.issuer, sub: identity.subject });
      const account = "00000000-0000-4000-8000-000000000012";
      mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: identity.subject });
      const fetcher = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(new Response(JSON.stringify({ account, identity })));

      const result = resolveRequestSession(
        { headers: { authorization: `Bearer ${token}` } },
        {
          appId: "app",
          accountRegistry: "https://core.example/apps/app/accounts",
          backendSecret: "backend-secret",
          allowLocalFirstAuth,
        },
      );

      if (accepted) {
        await expect(result).resolves.toMatchObject({
          issuer: identity.issuer,
          user_id: identity.subject,
          account_id: account,
          authMode: "local-first",
        });
        expect(mocks.verifyLocalFirstIdentityProof).toHaveBeenCalledWith(token, "app");
        expect(fetcher).toHaveBeenCalledTimes(1);
        expect(fetcher).toHaveBeenCalledWith(
          "https://core.example/apps/app/admin/accounts/found-local-first",
          expect.objectContaining({
            method: "POST",
            headers: expect.objectContaining({ "X-Jazz-Backend-Secret": "backend-secret" }),
          }),
        );
      } else {
        await expect(result).rejects.toThrow(/local-first authentication is disabled/);
        expect(mocks.verifyLocalFirstIdentityProof).not.toHaveBeenCalled();
        expect(fetcher).not.toHaveBeenCalled();
      }
    },
  );

  it("honours a registry rejection when local-first auth is explicitly enabled in production", async () => {
    vi.stubEnv("NODE_ENV", "production");
    const userId = "11111111-1111-1111-1111-111111111111";
    const token = makeUnsignedJwt({ iss: "urn:jazz:local-first", sub: userId });
    mocks.verifyLocalFirstIdentityProof.mockReturnValue({ ok: true, id: userId });
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("identity_not_authorized", { status: 403 }));

    await expect(
      resolveRequestSession(
        { headers: { authorization: `Bearer ${token}` } },
        {
          appId: "app",
          accountRegistry: "https://core.example/apps/app/accounts",
          backendSecret: "backend-secret",
          allowLocalFirstAuth: true,
        },
      ),
    ).rejects.toBeInstanceOf(RequestAuthenticationError);
    expect(mocks.verifyLocalFirstIdentityProof).toHaveBeenCalledWith(token, "app");
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher).toHaveBeenCalledWith(
      "https://core.example/apps/app/admin/accounts/found-local-first",
      expect.objectContaining({
        headers: expect.objectContaining({ "X-Jazz-Backend-Secret": "backend-secret" }),
      }),
    );
  });

  it("rejects local-first JWTs when allowLocalFirstAuth is disabled", async () => {
    const appId = "local-first-disabled-app";
    const token = makeUnsignedJwt({
      iss: "urn:jazz:local-first",
      sub: "22222222-2222-2222-2222-222222222222",
      auth_mode: "local-first",
    });

    await expect(
      resolveRequestSession(
        {
          headers: {
            authorization: `Bearer ${token}`,
          },
        },
        {
          appId,
          allowLocalFirstAuth: false,
        },
      ),
    ).rejects.toThrow(/local-first/i);
  });

  it("recognises the error from another copy of jazz-tools by name", () => {
    const foreign = Object.assign(new Error("Missing or invalid Authorization header"), {
      name: "RequestAuthenticationError",
    });
    expect(isRequestAuthenticationError(foreign)).toBe(true);
    expect(isRequestAuthenticationError(new Error("registry down"))).toBe(false);
    expect(isRequestAuthenticationError("RequestAuthenticationError")).toBe(false);
  });
});
