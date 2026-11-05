import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { betterAuth } from "better-auth";
import { hashPassword } from "better-auth/crypto";
import { schema as s } from "../schema-namespace.js";
import { createJazz, sqlite, type Jazz } from "../backend/create-jazz.js";
import { jazzAdapter } from "./index.js";
import { structuralSchemaHash } from "../dev/schema-utils.js";
import type { ClientMigrations } from "../runtime/client-migrations.js";
import type { WasmSchema } from "../drivers/types.js";
import { legacyDefinition, currentDefinition } from "./fixtures/issuer-upgrade-schema.js";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
const directory = join(root, "examples/auth-betterauth-chat/migrations");
const predecessor = "8acd8a85eef5015347cfc6ee0024ea14148aebb59869345c4d53a8e390bc6b88";
const current = "c95f3dba180615f83246729f0dceceab4b384ad00a92f30c59964f725524d822";

function loadBundle(): ClientMigrations {
  const source = readFileSync(join(directory, "index.ts"), "utf8");
  return JSON.parse(
    source.slice(
      source.indexOf("{", source.indexOf("export const migrations")),
      source.lastIndexOf(";"),
    ),
  );
}

it("upgrades an existing issuer-era account through createJazz and signs in after SQLite reopening", async () => {
  const bundle = loadBundle();
  const oldSchema: WasmSchema = JSON.parse(
    readFileSync(join(directory, "snapshots", predecessor.slice(0, 12) + ".json"), "utf8"),
  );
  const newSchema: WasmSchema = JSON.parse(
    readFileSync(join(directory, "snapshots", current.slice(0, 12) + ".json"), "utf8"),
  );
  expect(structuralSchemaHash(oldSchema)).toBe(predecessor);
  expect(structuralSchemaHash(newSchema)).toBe(current);
  const oldApp = s.defineApp(legacyDefinition, {
    migrations: {
      schemas: bundle.schemas.filter((version) => version.hash === predecessor),
      migrations: [],
    },
  });
  const newApp = s.defineApp(currentDefinition, { migrations: bundle });
  expect(structuralSchemaHash(oldApp.wasmSchema)).toBe(predecessor);
  expect(structuralSchemaHash(newApp.wasmSchema)).toBe(current);
  const permissions = s.definePermissions(oldApp, ({ policy }) => {
    for (const name of [
      "better_auth_user",
      "better_auth_session",
      "better_auth_account",
      "better_auth_verification",
      "better_auth_jwks",
      "messages",
    ] as const) {
      policy[name].allowRead.never();
      policy[name].allowInsert.never();
      policy[name].allowUpdate.never();
      policy[name].allowDelete.never();
    }
  });
  const path = await mkdtemp(join(tmpdir(), "jazz-better-auth-upgrade-"));
  const store = sqlite({ path, appId: randomUUID() });
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
      database: jazzAdapter({ db: () => db, schema: newApp.wasmSchema }),
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
