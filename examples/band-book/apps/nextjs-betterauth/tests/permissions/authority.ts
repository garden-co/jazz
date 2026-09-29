import { expect } from "vitest";
import type { Db } from "jazz-tools";
import { createJazzContext } from "jazz-tools/backend";
import { deploy, startLocalJazzServer } from "jazz-tools/testing";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

/**
 * A local Jazz authority with BandBook's schema and policies, plus the two
 * kinds of caller the app has: the trusted backend (bootstrap and invite
 * routes) and signed-in people.
 */
export async function startAuthority() {
  const backendSecret = "band-book-test-backend-secret";
  const adminSecret = "band-book-test-admin-secret";
  const server = await startLocalJazzServer({ backendSecret, adminSecret });
  await deploy({
    appId: server.appId,
    serverUrl: server.url,
    adminSecret,
    schema: app,
    permissions,
  });
  const context = createJazzContext({
    appId: server.appId,
    app,
    permissions,
    driver: { type: "memory" },
    serverUrl: server.url,
    backendSecret,
    env: "test",
  });
  return {
    backend: context.asBackend() as Db,
    as(subject: string, account: string, issuer = "https://band-book.test"): Db {
      return context.forSession({
        issuer,
        user_id: subject,
        account_id: account,
        claims: {},
        authMode: "external",
      });
    },
    async shutdown() {
      await context.shutdown();
      await server.stop();
    },
  };
}

export async function expectRejected(write: {
  wait(options: { tier: "global" }): Promise<unknown>;
}) {
  await expect(write.wait({ tier: "global" })).rejects.toThrow(
    /AuthorizationDenied|Write rejected by server authorization/,
  );
}
