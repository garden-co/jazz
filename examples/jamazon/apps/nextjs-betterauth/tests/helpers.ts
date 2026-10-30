import type { Db } from "jazz-tools";
import { createPolicyTestApp, type PolicyTestApp, type TestDb } from "jazz-tools/testing";
import { expect } from "vitest";
import permissions from "../permissions";
import { app } from "../schema";

export const ISSUER = "https://jamazon.test";

/** A test app plus the backend-authority Db the store's server code runs with. */
export async function startStore(): Promise<{ testApp: PolicyTestApp; backend: Db }> {
  const testApp = await createPolicyTestApp(app, permissions, expect);
  let backend: Db | undefined;
  // `seed` hands its callback the backend-authority Db; keep it for the
  // server-side workflow under test.
  await testApp.seed((db) => {
    backend = db;
    return db.insert(app.categories, { slug: "probe", name: "Probe", blurb: "", position: 99 });
  });
  return { testApp, backend: backend! };
}

/** A signed-in shopper: an external identity admitted to its own account. */
export function shopper(testApp: PolicyTestApp, name: string): { account: string; db: TestDb } {
  const account = crypto.randomUUID();
  const db = testApp.as({
    issuer: ISSUER,
    user_id: name,
    account_id: account,
    claims: {},
    authMode: "external",
  });
  return { account, db };
}

/**
 * A guest: the local-first account every visitor gets on first load, before
 * signing in. A real guest session: no issuer, no admitted account and no
 * custom claims; the test app derives its account from the device key.
 */
export function guest(testApp: PolicyTestApp, device: string): { account: string; db: TestDb } {
  const session = { user_id: device, claims: {}, authMode: "local-first" } as const;
  return { account: testApp.accountFor(session), db: testApp.as(session) };
}
