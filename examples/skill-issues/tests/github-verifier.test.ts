import { afterEach, describe, expect, it, vi } from "vitest";
import { createVerifierApp } from "../src/server/server.js";
import { exchangeDeviceCode, fetchGitHubUser } from "../src/server/github.js";
import { requestJson } from "./support/http.js";
import type { VerifiedUser } from "../src/repository.js";

describe("GitHub verifier", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("verifies GitHub device auth and records the Jazz user binding", async () => {
    const savedUsers: VerifiedUser[] = [];
    const app = createVerifierApp({
      github: {
        exchangeDeviceCode: vi.fn(async (deviceCode: string) => {
          expect(deviceCode).toBe("device-code-123");
          return { accessToken: "github-token" };
        }),
        fetchUser: vi.fn(async (accessToken: string) => {
          expect(accessToken).toBe("github-token");
          return { id: "1001", login: "alice" };
        }),
      },
      verifyJazzProof: vi.fn(async (proof: string) => {
        expect(proof).toBe("proof-token");
        return { jazzUserId: "alice-jazz-id" };
      }),
      openBackendRepository: vi.fn(async () => ({
        async upsertVerifiedUser(user: VerifiedUser) {
          savedUsers.push(user);
          return user;
        },
      })),
    });

    const response = await requestJson(app, "POST", "/auth/github/complete", {
      deviceCode: "device-code-123",
      jazzProof: "proof-token",
    });

    expect(response).toMatchObject({
      statusCode: 200,
      body: {
        id: "alice-jazz-id",
        githubLogin: "alice",
      },
    });
    expect(savedUsers).toEqual([
      {
        id: "alice-jazz-id",
        githubUserId: "1001",
        githubLogin: "alice",
        verifiedAt: expect.any(String),
      },
    ]);
  });

  it("rejects requests missing the device code or Jazz proof", async () => {
    const app = createVerifierApp({
      github: {
        exchangeDeviceCode: vi.fn(async () => ({ accessToken: "github-token" })),
        fetchUser: vi.fn(async () => ({ id: "1001", login: "alice" })),
      },
      verifyJazzProof: vi.fn(async () => ({ jazzUserId: "alice-jazz-id" })),
      openBackendRepository: vi.fn(async () => ({
        async upsertVerifiedUser(user: VerifiedUser) {
          return user;
        },
      })),
    });

    const missingDeviceCode = await requestJson(app, "POST", "/auth/github/complete", {
      jazzProof: "proof-token",
    });
    const missingProof = await requestJson(app, "POST", "/auth/github/complete", {
      deviceCode: "device-code-123",
    });

    expect(missingDeviceCode.statusCode).toBe(400);
    expect(missingProof.statusCode).toBe(400);
    expect(missingDeviceCode.body).toEqual({
      error: "deviceCode and jazzProof are required",
    });
    expect(missingProof.body).toEqual({
      error: "deviceCode and jazzProof are required",
    });
  });

  it("rejects an invalid Jazz proof before exchanging the GitHub device code", async () => {
    const exchangeDeviceCode = vi.fn(async () => ({ accessToken: "github-token" }));
    const app = createVerifierApp({
      github: {
        exchangeDeviceCode,
        fetchUser: vi.fn(async () => ({ id: "1001", login: "alice" })),
      },
      verifyJazzProof: vi.fn(async () => {
        throw new Error("raw proof implementation detail");
      }),
      openBackendRepository: vi.fn(async () => ({
        async upsertVerifiedUser(user: VerifiedUser) {
          return user;
        },
      })),
    });

    const response = await requestJson(app, "POST", "/auth/github/complete", {
      deviceCode: "device-code-123",
      jazzProof: "bad-proof",
    });

    expect(response).toEqual({
      statusCode: 401,
      body: { error: "Invalid Jazz identity proof" },
    });
    expect(exchangeDeviceCode).not.toHaveBeenCalled();
  });

  it("does not leak unexpected backend errors", async () => {
    const app = createVerifierApp({
      github: {
        exchangeDeviceCode: vi.fn(async () => ({ accessToken: "github-token" })),
        fetchUser: vi.fn(async () => ({ id: "1001", login: "alice" })),
      },
      verifyJazzProof: vi.fn(async () => ({ jazzUserId: "alice-jazz-id" })),
      openBackendRepository: vi.fn(async () => {
        throw new Error("database password appeared here");
      }),
    });

    const response = await requestJson(app, "POST", "/auth/github/complete", {
      deviceCode: "device-code-123",
      jazzProof: "proof-token",
    });

    expect(response).toEqual({
      statusCode: 500,
      body: { error: "Verifier request failed" },
    });
  });

  it("fetches the GitHub user with the versioned GitHub JSON accept header", async () => {
    const fetch = vi.fn(async () => {
      return new Response(JSON.stringify({ id: 1001, login: "alice" }), {
        headers: { "content-type": "application/json" },
      });
    });
    vi.stubGlobal("fetch", fetch);

    await expect(fetchGitHubUser("github-token")).resolves.toEqual({
      id: "1001",
      login: "alice",
    });

    expect(fetch).toHaveBeenCalledWith("https://api.github.com/user", {
      headers: {
        accept: "application/vnd.github+json",
        authorization: "Bearer github-token",
      },
    });
  });

  it("exchanges a GitHub device code without sending a client secret", async () => {
    const fetch = vi.fn(async () => {
      return new Response(JSON.stringify({ access_token: "github-token" }), {
        headers: { "content-type": "application/json" },
      });
    });
    vi.stubGlobal("fetch", fetch);

    await expect(
      exchangeDeviceCode({
        clientId: "github-client-id",
        deviceCode: "device-code-123",
      }),
    ).resolves.toEqual({ accessToken: "github-token" });

    expect(fetch).toHaveBeenCalledWith("https://github.com/login/oauth/access_token", {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        client_id: "github-client-id",
        device_code: "device-code-123",
        grant_type: "urn:ietf:params:oauth:grant-type:device_code",
      }),
    });
  });
});
