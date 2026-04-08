import { afterEach, describe, expect, it } from "vitest";
import { startJazzHostedAuthServer } from "./server.js";

const activeHandles: Array<{ close(): Promise<void> }> = [];

afterEach(async () => {
  await Promise.all(
    activeHandles
      .splice(0)
      .reverse()
      .map((handle) => handle.close()),
  );
});

describe("startJazzHostedAuthServer", () => {
  it("serves hosted auth pages, health, and JWKS from the bundled auth server", async () => {
    const server = await startJazzHostedAuthServer({
      baseURL: "http://127.0.0.1:4100",
      port: 0,
      secret: "development-secret",
    });
    activeHandles.push(server);

    const healthResponse = await fetch(`${server.url}/health`);
    await expect(healthResponse.json()).resolves.toEqual({ status: "ok" });

    const signInResponse = await fetch(`${server.url}/auth/sign-in?redirectTo=/app`);
    expect(signInResponse.status).toBe(200);
    const signInHtml = await signInResponse.text();
    expect(signInHtml).toContain("Sign in");
    expect(signInHtml).toContain("/auth/sign-in/submit");

    const jwksResponse = await fetch(`${server.url}/.well-known/jwks.json`);
    expect(jwksResponse.status).toBe(200);
    const jwks = (await jwksResponse.json()) as { keys?: Array<{ kid?: string }> };
    expect(Array.isArray(jwks.keys)).toBe(true);
    expect(jwks.keys?.[0]?.kid).toBeTruthy();
  });
});
