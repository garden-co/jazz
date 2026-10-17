import { expect } from "vitest";
import type { Db } from "jazz-tools";
import { createPolicyTestApp } from "jazz-tools/testing";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

/**
 * A local Jazz authority with BandBook's schema and policies, plus the two
 * kinds of caller the app has: the trusted backend (bootstrap and invite
 * routes) and signed-in people.
 */
export async function startAuthority() {
  const testApp = await createPolicyTestApp(app, permissions, expect);
  // `seed` hands its callback the backend Db. Keep it for the server-side
  // helpers under test, which run their own exclusive transactions.
  let backend: Db | undefined;
  await testApp.seed((db) => {
    backend = db;
    return db.insert(app.workspaces, { name: "Authority warm-up" });
  });
  return {
    backend: backend!,
    as(subject: string, account: string, issuer = "https://band-book.test"): Db {
      return testApp.as({
        issuer,
        user_id: subject,
        account_id: account,
        claims: {},
        authMode: "external",
      });
    },
    shutdown: () => testApp.shutdown(),
  };
}
