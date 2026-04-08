import { afterEach, describe, expect, it, vi } from "vitest";
import { createJazzAuthClient, jazzAuthTables } from "./index.js";
import {
  createJazzHostedAuth,
  handleJazzHostedSignIn,
  handleJazzHostedSignOut,
  handleJazzHostedSignUp,
} from "./hosted/index.js";

describe("jazzAuthTables", () => {
  it("returns the Better Auth core and JWT tables Jazz expects", () => {
    const tables = jazzAuthTables();

    expect(Object.keys(tables)).toEqual([
      "authUsers",
      "authAccounts",
      "authSessions",
      "authVerifications",
      "authRateLimits",
      "authJwks",
    ]);
    expect(tables.authUsers.columns).toHaveProperty("principalId");
    expect(tables.authAccounts.columns).toHaveProperty("providerId");
    expect(tables.authSessions.columns).toHaveProperty("token");
    expect(tables.authVerifications.columns).toHaveProperty("identifier");
    expect(tables.authRateLimits.columns).toHaveProperty("key");
    expect(tables.authJwks.columns).toHaveProperty("publicKey");
  });
});

describe("createJazzAuthClient", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("builds hosted sign-in and sign-up redirect URLs", () => {
    const client = createJazzAuthClient({
      baseURL: "https://auth.example.com",
    });

    expect(client.getSignInUrl({ redirectTo: "/app" })).toBe(
      "https://auth.example.com/auth/sign-in?redirectTo=%2Fapp",
    );
    expect(client.getSignUpUrl({ redirectTo: "/welcome" })).toBe(
      "https://auth.example.com/auth/sign-up?redirectTo=%2Fwelcome",
    );
  });

  it("redirects through the hosted sign-in flow and aliases login()", () => {
    const assign = vi.fn();
    const client = createJazzAuthClient({
      baseURL: "https://auth.example.com",
      location: { assign },
    });

    expect(client.signIn({ redirectTo: "/app" })).toBe(
      "https://auth.example.com/auth/sign-in?redirectTo=%2Fapp",
    );
    expect(client.login({ redirectTo: "/app" })).toBe(
      "https://auth.example.com/auth/sign-in?redirectTo=%2Fapp",
    );
    expect(assign).toHaveBeenCalledTimes(2);
  });

  it("fetches the hosted Jazz JWT from the Better Auth token endpoint", async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(JSON.stringify({ token: "jwt-123" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });
    const client = createJazzAuthClient({
      baseURL: "https://auth.example.com",
      fetch: fetchMock as typeof fetch,
    });

    await expect(client.getJwt()).resolves.toBe("jwt-123");
    expect(fetchMock).toHaveBeenCalledWith("https://auth.example.com/api/auth/token", {
      credentials: "include",
      headers: { accept: "application/json" },
      method: "GET",
    });
  });
});

describe("createJazzHostedAuth", () => {
  it("creates Better Auth handlers with Jazz defaults", () => {
    const hosted = createJazzHostedAuth({
      baseURL: "https://auth.example.com",
      secret: "development-secret",
    });

    expect(hosted.apiBasePath).toBe("/api/auth");
    expect(hosted.hostedBasePath).toBe("/auth");
    expect(typeof hosted.handlers.GET).toBe("function");
    expect(typeof hosted.handlers.POST).toBe("function");
  });

  it("redirects successful hosted sign-in responses and preserves every auth cookie", async () => {
    const headers = new Headers();
    headers.append("set-cookie", "session=abc; Path=/; HttpOnly");
    headers.append("set-cookie", "csrf=def; Path=/; Secure");

    const response = await handleJazzHostedSignIn(
      {
        apiBasePath: "/api/auth",
        auth: {
          handler: vi.fn(async () => {
            return new Response(JSON.stringify({ redirect: true, url: "/app" }), {
              headers,
              status: 200,
            });
          }),
        },
        handlers: {} as never,
        hostedBasePath: "/auth",
      },
      new Request("https://auth.example.com/auth/sign-in/submit", {
        body: new URLSearchParams({
          email: "alice@example.com",
          password: "secret-password",
          redirectTo: "/chat",
        }),
        headers: { "content-type": "application/x-www-form-urlencoded" },
        method: "POST",
      }),
    );

    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe("https://auth.example.com/app");
    expect(response.headers.getSetCookie()).toEqual([
      "session=abc; Path=/; HttpOnly",
      "csrf=def; Path=/; Secure",
    ]);
  });

  it("redirects failed hosted sign-up responses back to the hosted page with an error", async () => {
    const response = await handleJazzHostedSignUp(
      {
        apiBasePath: "/api/auth",
        auth: {
          handler: vi.fn(async () => {
            return new Response(JSON.stringify({ message: "Email already exists" }), {
              headers: { "content-type": "application/json" },
              status: 400,
            });
          }),
        },
        handlers: {} as never,
        hostedBasePath: "/auth",
      },
      new Request("https://auth.example.com/auth/sign-up/submit", {
        body: new URLSearchParams({
          email: "alice@example.com",
          name: "Alice",
          password: "secret-password",
          redirectTo: "/chat",
        }),
        headers: { "content-type": "application/x-www-form-urlencoded" },
        method: "POST",
      }),
    );

    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe(
      "https://auth.example.com/auth/sign-up?redirectTo=%2Fchat&error=Email+already+exists",
    );
  });

  it("posts sign-out through Better Auth and redirects back to the app", async () => {
    const authHandler = vi.fn(async () => {
      return new Response(JSON.stringify({ redirect: true, url: "/signed-out" }), {
        headers: { "set-cookie": "session=; Max-Age=0; Path=/; HttpOnly" },
        status: 200,
      });
    });

    const response = await handleJazzHostedSignOut(
      {
        apiBasePath: "/api/auth",
        auth: {
          handler: authHandler,
        },
        handlers: {} as never,
        hostedBasePath: "/auth",
      },
      new Request("https://auth.example.com/auth/sign-out?redirectTo=/bye", {
        method: "GET",
      }),
    );

    expect(authHandler).toHaveBeenCalledTimes(1);
    const forwardedRequest = authHandler.mock.calls[0]?.[0] as Request;
    expect(forwardedRequest.method).toBe("POST");
    expect(forwardedRequest.url).toBe("https://auth.example.com/api/auth/sign-out");
    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe("https://auth.example.com/signed-out");
  });
});
