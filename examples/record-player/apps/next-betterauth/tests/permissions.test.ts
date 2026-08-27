import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../schema";
import permissions from "../permissions";

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp.shutdown();
});

describe("RecordPlayer Better Auth storage boundary", () => {
  it("keeps Better Auth rows unreadable and unwritable from an authenticated app session", async () => {
    await testApp.seed((db) =>
      db.insert(app.better_auth_user, {
        name: "Private account",
        email: "private@example.invalid",
        emailVerified: true,
        createdAt: new Date(0),
        updatedAt: new Date(0),
      }),
    );
    const browser = testApp.as({
      issuer: "https://auth.record-player.example",
      user_id: "listener",
      claims: {},
      authMode: "external",
    });

    expect(await browser.all(app.better_auth_user)).toEqual([]);
    await browser.expectDenied((db) =>
      db.insert(app.better_auth_user, {
        name: "Injected account",
        email: "attacker@example.invalid",
        emailVerified: true,
        createdAt: new Date(0),
        updatedAt: new Date(0),
      }),
    );
  });
});
