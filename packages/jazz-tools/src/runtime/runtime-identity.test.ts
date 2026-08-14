import { describe, expect, it } from "vitest";
import type { DbConfig } from "./db.js";
import { resolveRuntimeIdentity } from "./runtime-identity.js";

function toBase64Url(value: unknown): string {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function makeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "HS256", typ: "JWT" })}.${toBase64Url(payload)}.signature`;
}

function hex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}

describe("resolveRuntimeIdentity", () => {
  it("pins the existing persistent-browser derivation", () => {
    const config: DbConfig = {
      appId: "chat-app",
      env: "prod",
      userBranch: "inbox",
      jwtToken: makeJwt({ sub: "alice@example.com" }),
    };

    const first = resolveRuntimeIdentity(config, "chat-app::alice%40example.com");
    const reopened = resolveRuntimeIdentity(config, "chat-app::alice%40example.com");

    expect(hex(first.node)).toBe("7f91c233519ca38bdfafcc8dad0875e3");
    expect(hex(first.author)).toBe("aa006163b22e5d268740b3bfb50ec29c");
    expect(reopened).toEqual(first);
  });

  it("includes the logical database name in the persistent node identity", () => {
    const config: DbConfig = { appId: "chat-app" };

    const first = resolveRuntimeIdentity(config, "first");
    const second = resolveRuntimeIdentity(config, "second");

    expect(first.node).not.toEqual(second.node);
    expect(first.author).toEqual(second.author);
  });

  it("uses UUID subjects directly as the author identity", () => {
    const subject = "00112233-4455-6677-8899-aabbccddeeff";
    const identity = resolveRuntimeIdentity(
      {
        appId: "chat-app",
        cookieSession: { user_id: subject, claims: {}, authMode: "external" },
      },
      "chat-app",
    );

    expect(hex(identity.author)).toBe("00112233445566778899aabbccddeeff");
  });

  it("derives non-UUID subjects with the canonical URL-namespace UUID v5", () => {
    const identity = resolveRuntimeIdentity(
      {
        appId: "chat-app",
        jwtToken: makeJwt({ sub: "better-auth-user-1" }),
      },
      "chat-app",
    );

    expect(hex(identity.author)).toBe("8bcef93bc5d153a0ab81d3eb64b487d0");
  });
});
