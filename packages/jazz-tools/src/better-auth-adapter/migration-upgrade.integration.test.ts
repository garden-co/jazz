import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { betterAuth } from "better-auth";
import { hashPassword } from "better-auth/crypto";
import { createJazz, type Jazz } from "../backend/create-jazz.js";
import { files } from "../backend/blobs.js";
import { sqlite } from "../backend/native-provider.js";
import { compileTestApp, testApp } from "../testing/test-app.js";
import { jazzAdapter } from "./index.js";
import {
  currentAppFile,
  currentHash,
  denyAllPermissions,
  legacyAppFile,
  legacyHash,
} from "./fixtures/issuer-upgrade.js";

it("upgrades an existing issuer-era account through createJazz and signs in after SQLite reopening", async () => {
  // The issuer-era app ships only its own version; the current one ships the
  // legacy version in its history and the migration dropping `issuer`.
  expect(compileTestApp(legacyAppFile).schema).toMatchObject({ current: legacyHash });
  expect(compileTestApp(currentAppFile).schema).toMatchObject({ current: currentHash });
  const oldApp = testApp(legacyAppFile);
  const newApp = testApp(currentAppFile);
  const permissions = denyAllPermissions;
  const path = await mkdtemp(join(tmpdir(), "jazz-better-auth-upgrade-"));
  const store = sqlite({ path, blobs: files() });
  let jazz: Jazz | undefined;
  try {
    // Seed an actual predecessor store, with real persistent user/account rows.
    // The password fixture uses Better Auth's supported scrypt representation;
    // this is not a claim that the old Better Auth package was executed here.
    jazz = await createJazz({ app: oldApp, permissions, store });
    const password = "existing-upgrade-fixture-password";
    const digest = await hashPassword(password);
    const user = jazz.db.insert(oldApp.better_auth_user, {
      name: "Upgrade fixture",
      email: "upgrade@example.test",
      emailVerified: true,
      createdAt: new Date(0),
      updatedAt: new Date(0),
    });
    await user.wait({ tier: "global" });
    const account = jazz.db.insert(oldApp.better_auth_account, {
      issuer: "fixture:legacy-credential",
      providerId: "credential",
      accountId: user.value.id,
      userId: user.value.id,
      password: digest,
      createdAt: new Date(0),
      updatedAt: new Date(0),
    });
    await account.wait({ tier: "global" });
    const expectedUser = user.value.id;
    const expectedAccount = account.value.id;
    await jazz.close();
    jazz = undefined;

    // Normal createJazz startup publishes the connected shipped bundle.
    jazz = await createJazz({ app: newApp, permissions, store });
    expect(
      await jazz.db.one(newApp.better_auth_account.where({ id: expectedAccount })),
    ).toMatchObject({
      id: expectedAccount,
      providerId: "credential",
      accountId: expectedUser,
      userId: expectedUser,
      password: digest,
    });
    // Backend Db is bound to its configured bundle; a table from oldApp does
    // not switch a current Db to a legacy view. Close each owner before using
    // public createJazz to republish the legacy/current bundle on this store.
    // Both handles use this same current runtime/library, not a package downgrade.
    await jazz.close();
    jazz = undefined;
    jazz = await createJazz({ app: oldApp, permissions, store });
    // The backwards projection retains a previously stored issuer verbatim.
    expect(
      await jazz.db.one(oldApp.better_auth_account.where({ id: expectedAccount })),
    ).toMatchObject({ issuer: "fixture:legacy-credential" });
    await jazz.close();
    jazz = undefined;
    jazz = await createJazz({ app: newApp, permissions, store });
    const db = jazz.db;
    const auth = betterAuth({
      baseURL: "http://localhost:3000",
      secret: "better-auth-migration-local-fixture-secret",
      database: jazzAdapter({ db: () => db, schema: newApp }),
      emailAndPassword: { enabled: true },
      logger: { disabled: true },
    });
    const signed = await auth.api.signInEmail({
      body: { email: "upgrade@example.test", password },
    });
    expect(signed.user.id).toBe(expectedUser);
    expect(signed.token).toBeTruthy();
    const added = await auth.api.signUpEmail({
      body: { name: "New fixture", email: "new-upgrade@example.test", password },
    });
    await jazz.close();
    jazz = undefined;
    jazz = await createJazz({ app: oldApp, permissions, store });
    const newAccount = await jazz.db.one(
      oldApp.better_auth_account.where({ userId: added.user.id }),
    );
    expect(newAccount).toMatchObject({
      issuer: "",
      providerId: "credential",
      accountId: added.user.id,
    });
    await jazz.close();
    jazz = undefined;
    jazz = await createJazz({ app: newApp, permissions, store });
    expect(
      await jazz.db.one(newApp.better_auth_account.where({ id: expectedAccount })),
    ).toMatchObject({ password: digest });
  } finally {
    await jazz?.close();
    await rm(path, { recursive: true, force: true });
  }
}, 60_000);
