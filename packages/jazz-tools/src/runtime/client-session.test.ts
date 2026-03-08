import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  deriveLocalPrincipalId,
  deriveLocalPrincipalIdSync,
  resolveClientSession,
  resolveClientSessionSync,
} from "./client-session.js";

function toBase64Url(value: string): string {
  return Buffer.from(value, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "none", typ: "JWT" };
  return `${toBase64Url(JSON.stringify(header))}.${toBase64Url(JSON.stringify(payload))}.`;
}

describe("client session resolution", () => {
  it("prefers jazz_principal_id from JWT when present", async () => {
    const jwt = makeJwt({
      sub: "user-subject",
      jazz_principal_id: "principal-123",
      iss: "https://issuer.example",
      claims: { role: "editor" },
    });

    const session = await resolveClientSession({
      appId: "app-jwt-principal",
      jwtToken: jwt,
      localAuthMode: "demo",
      localAuthToken: "device-a",
    });

    expect(session).toEqual({
      user_id: "principal-123",
      claims: {
        role: "editor",
        auth_mode: "external",
        subject: "user-subject",
        issuer: "https://issuer.example",
      },
    });
  });

  it("falls back to JWT sub when principal claim is absent", async () => {
    const jwt = makeJwt({
      sub: "user-subject",
      claims: { team: "eng" },
    });

    const session = await resolveClientSession({
      appId: "app-jwt-sub",
      jwtToken: jwt,
    });

    expect(session).toEqual({
      user_id: "user-subject",
      claims: {
        team: "eng",
        auth_mode: "external",
        subject: "user-subject",
      },
    });
  });

  it("derives local principal id with the server-compatible hash format", async () => {
    const appId = "app-local";
    const mode = "anonymous";
    const token = "device-token";
    const digest = createHash("sha256")
      .update(`${appId}:${mode}:${token}`, "utf8")
      .digest("base64");
    const expected = `local:${digest.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "")}`;

    expect(await deriveLocalPrincipalId(appId, mode, token)).toBe(expected);
    expect(deriveLocalPrincipalIdSync(appId, mode, token)).toBe(expected);

    const session = await resolveClientSession({
      appId,
      localAuthMode: mode,
      localAuthToken: token,
    });
    expect(session).toEqual({
      user_id: expected,
      claims: {
        auth_mode: "local",
        local_mode: mode,
      },
    });
    expect(
      resolveClientSessionSync({
        appId,
        localAuthMode: mode,
        localAuthToken: token,
      }),
    ).toEqual({
      user_id: expected,
      claims: {
        auth_mode: "local",
        local_mode: mode,
      },
    });
  });

  it("returns null when no auth is configured", async () => {
    const session = await resolveClientSession({ appId: "no-auth" });
    expect(session).toBeNull();
    expect(resolveClientSessionSync({ appId: "no-auth" })).toBeNull();
  });
});
