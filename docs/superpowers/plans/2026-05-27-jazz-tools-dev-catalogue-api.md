# Jazz Tools Dev Catalogue API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export programmatic schema, permissions, migration, and deploy operations from `jazz-tools/dev` while preserving CLI output and existing `pushSchemaCatalogue` behavior.

**Architecture:** Add a dev catalogue module that owns project-level publication logic and returns structured results/events. Keep `cli.ts` responsible for argument parsing and human-readable logging by wrapping the dev catalogue functions. Keep `pushSchemaCatalogue` as the lightweight dev auto-push compatibility path.

**Tech Stack:** TypeScript, NodeNext ESM, Vitest, existing Jazz schema DSL, existing catalogue HTTP helpers in `packages/jazz-tools/src/runtime/schema-fetch.ts`.

---

## File Structure

- Create `packages/jazz-tools/src/dev/catalogue.ts`
  - Public programmatic types and functions: `pushSchema`, `pushPermissions`, `pushMigration`, `deploy`, `pushSchemaCatalogue`.
  - Private helpers moved from `cli.ts` when they are required by migration/deploy logic.
  - No CLI argument parsing and no unconditional `console` calls.
- Modify `packages/jazz-tools/src/dev/dev-server.ts`
  - Keep `startLocalJazzServer` in this file.
  - Re-export `pushSchemaCatalogue` and its types from `./catalogue.js` for backward compatibility.
- Modify `packages/jazz-tools/src/dev/index.ts`
  - Export new public catalogue functions and result/option/event types from `./catalogue.js`.
- Modify `packages/jazz-tools/src/testing/local-jazz-server.ts`
  - Continue exporting `pushSchemaCatalogue` and related compatibility types through the existing testing path.
- Modify `packages/jazz-tools/src/cli.ts`
  - Import dev catalogue implementations.
  - Keep existing exported CLI-facing `pushMigration` and `deploy` wrappers returning `Promise<void>` and printing the current messages.
  - Keep argument parsing, env loading, usage text, and process exit handling in `cli.ts`.
- Create `packages/jazz-tools/src/dev/catalogue.test.ts`
  - Black-box tests for `jazz-tools/dev` programmatic exports and structured results.
  - Use public schema/permissions DSL in fixture files.
- Modify `packages/jazz-tools/src/dev/dev-server.test.ts`
  - Expand export compatibility checks for new functions.
- Modify `packages/jazz-tools/src/cli.test.ts`
  - Keep existing CLI behavior tests; adjust only imports or expectations made necessary by wrapper boundaries.

---

### Task 1: Add Dev Catalogue Export Tests

**Files:**

- Create: `packages/jazz-tools/src/dev/catalogue.test.ts`
- Modify: `packages/jazz-tools/src/dev/dev-server.test.ts`

- [ ] **Step 1: Write failing export compatibility tests**

Add this test file:

```ts
import { describe, expect, it } from "vitest";

describe("dev catalogue API exports", () => {
  it("exports project-level catalogue operations from jazz-tools/dev", async () => {
    const dev = await import("./index.js");

    expect(typeof dev.pushSchema).toBe("function");
    expect(typeof dev.pushPermissions).toBe("function");
    expect(typeof dev.pushMigration).toBe("function");
    expect(typeof dev.deploy).toBe("function");
    expect(typeof dev.pushSchemaCatalogue).toBe("function");
  });

  it("keeps pushSchemaCatalogue compatible across dev and testing entrypoints", async () => {
    const dev = await import("./index.js");
    const testing = await import("../testing/index.js");

    expect(testing.pushSchemaCatalogue).toBe(dev.pushSchemaCatalogue);
  });
});
```

Also update `packages/jazz-tools/src/dev/dev-server.test.ts` inside `it("exports the same functions from dev/index.ts", ...)`:

```ts
expect(typeof dev.pushSchema).toBe("function");
expect(typeof dev.pushPermissions).toBe("function");
expect(typeof dev.pushMigration).toBe("function");
expect(typeof dev.deploy).toBe("function");
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/catalogue.test.ts src/dev/dev-server.test.ts
```

Expected: FAIL because `pushSchema`, `pushPermissions`, `pushMigration`, and `deploy` are not exported from `dev/index.ts`.

- [ ] **Step 3: Commit failing tests**

```bash
git add packages/jazz-tools/src/dev/catalogue.test.ts packages/jazz-tools/src/dev/dev-server.test.ts
git commit -m "test: cover dev catalogue exports"
```

---

### Task 2: Introduce `dev/catalogue.ts` With Schema And Permissions Push

**Files:**

- Create: `packages/jazz-tools/src/dev/catalogue.ts`
- Modify: `packages/jazz-tools/src/dev/dev-server.ts`
- Modify: `packages/jazz-tools/src/dev/index.ts`
- Modify: `packages/jazz-tools/src/testing/local-jazz-server.ts`

- [ ] **Step 1: Add the catalogue module skeleton and public types**

Create `packages/jazz-tools/src/dev/catalogue.ts` with this structure:

```ts
import { loadCompiledSchema, type LoadedSchemaProject } from "../schema-loader.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
  type StoredPermissionsHead,
} from "../runtime/schema-fetch.js";

export type CatalogueEvent =
  | { type: "schema-loaded"; schemaFile: string }
  | { type: "schema-published"; hash: string; objectId?: string }
  | { type: "schema-skipped"; hash: string; reason: "already-stored" }
  | { type: "permissions-loaded"; permissionsFile: string }
  | { type: "permissions-published"; schemaHash: string; version?: number }
  | { type: "permissions-skipped"; reason: "missing-permissions-file" }
  | { type: "warning"; message: string };

export interface CatalogueProjectOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
  onEvent?: (event: CatalogueEvent) => void;
}

export interface PushSchemaOptions extends CatalogueProjectOptions {
  skipIfStored?: boolean;
}

export interface PushSchemaResult {
  hash: string;
  schemaFile: string;
  status: "published" | "already-stored";
  objectId?: string;
}

export interface PushPermissionsOptions extends CatalogueProjectOptions {
  schemaHash?: string;
}

export interface PushPermissionsResult {
  schemaHash: string;
  permissionsFile: string;
  previousHead: StoredPermissionsHead | null;
  head: StoredPermissionsHead | null;
}

export interface PushSchemaCatalogueOptions extends CatalogueProjectOptions {
  env?: string;
  userBranch?: string;
  enableLogs?: boolean;
}

export interface PushMigrationOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  migrationsDir: string;
  fromHash: string;
  toHash: string;
  onEvent?: (event: CatalogueEvent) => void;
}

export interface PushMigrationResult {
  fromHash: string;
  toHash: string;
  status: "published";
  filePath?: string;
}

export interface DeployOptions extends CatalogueProjectOptions {
  migrationsDir: string;
  noVerify?: boolean;
}

export interface DeployResult {
  schema: PushSchemaResult;
  migration?:
    | PushMigrationResult
    | { status: "already-connected"; fromHash: string; toHash: string };
  permissions?: PushPermissionsResult;
  warnings: string[];
}

function emit(options: { onEvent?: (event: CatalogueEvent) => void }, event: CatalogueEvent): void {
  options.onEvent?.(event);
}

function ensurePermissionsProject(compiled: LoadedSchemaProject): LoadedSchemaProject & {
  permissions: NonNullable<LoadedSchemaProject["permissions"]>;
  permissionsFile: string;
} {
  if (!compiled.permissions || !compiled.permissionsFile) {
    throw new Error(
      "No permissions found for this app. Create a permissions.ts file before using permissions commands.",
    );
  }

  return compiled as LoadedSchemaProject & {
    permissions: NonNullable<LoadedSchemaProject["permissions"]>;
    permissionsFile: string;
  };
}
```

- [ ] **Step 2: Implement `pushSchema` and compatibility `pushSchemaCatalogue`**

Append:

```ts
export async function pushSchema(options: PushSchemaOptions): Promise<PushSchemaResult> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });

  const result = await publishStoredSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
  });

  emit(options, { type: "schema-published", hash: result.hash, objectId: result.objectId });

  return {
    hash: result.hash,
    schemaFile: compiled.schemaFile,
    status: "published",
    objectId: result.objectId,
  };
}

export async function pushPermissions(
  options: PushPermissionsOptions,
): Promise<PushPermissionsResult> {
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  emit(options, { type: "permissions-loaded", permissionsFile: compiled.permissionsFile });

  const schemaHash = options.schemaHash;
  if (!schemaHash) {
    throw new Error(
      "Missing schema hash. Push or resolve the structural schema before pushing permissions.",
    );
  }

  const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  const { head } = await publishStoredPermissions(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash,
    permissions: compiled.permissions,
    expectedParentBundleObjectId: previousHead?.bundleObjectId ?? null,
  });

  emit(options, { type: "permissions-published", schemaHash, version: head?.version });

  return {
    schemaHash,
    permissionsFile: compiled.permissionsFile,
    previousHead,
    head,
  };
}

export async function pushSchemaCatalogue(
  options: PushSchemaCatalogueOptions,
): Promise<{ hash: string }> {
  const schema = await pushSchema(options);
  const compiled = await loadCompiledSchema(options.schemaDir);

  if (compiled.permissions) {
    const { head } = await fetchPermissionsHead(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
    });
    await publishStoredPermissions(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schemaHash: schema.hash,
      permissions: compiled.permissions,
      expectedParentBundleObjectId: head?.bundleObjectId ?? null,
    });
  }

  if (options.enableLogs === true) {
    console.log(
      `[jazz-schema-push] published ${schema.hash} from ${schema.schemaFile} to ${options.serverUrl}`,
    );
  }

  return { hash: schema.hash };
}

export async function pushMigration(_options: PushMigrationOptions): Promise<PushMigrationResult> {
  throw new Error("pushMigration is not implemented yet.");
}

export async function deploy(_options: DeployOptions): Promise<DeployResult> {
  throw new Error("deploy is not implemented yet.");
}
```

This deliberately keeps `pushSchemaCatalogue` behavior close to the current implementation, including `enableLogs`.

- [ ] **Step 3: Re-export compatibility from `dev-server.ts`**

Remove the schema publication imports and local `PushSchemaCatalogueOptions` / `pushSchemaCatalogue` implementation from `packages/jazz-tools/src/dev/dev-server.ts`.

Add near the top-level exports:

```ts
export {
  pushSchemaCatalogue,
  type PushSchemaCatalogueOptions,
  type PushSchemaCatalogueOptions as PushSchemaOptions,
} from "./catalogue.js";
```

- [ ] **Step 4: Export new API from `dev/index.ts`**

Add:

```ts
export {
  deploy,
  pushMigration,
  pushPermissions,
  pushSchema,
  pushSchemaCatalogue,
  type CatalogueEvent,
  type DeployOptions,
  type DeployResult,
  type PushMigrationOptions,
  type PushMigrationResult,
  type PushPermissionsOptions,
  type PushPermissionsResult,
  type PushSchemaCatalogueOptions,
  type PushSchemaOptions,
  type PushSchemaResult,
} from "./catalogue.js";
```

- [ ] **Step 5: Keep testing path pointed at dev catalogue**

Modify `packages/jazz-tools/src/testing/local-jazz-server.ts` to continue re-exporting through `../dev/dev-server.js`. No caller should need an import change.

- [ ] **Step 6: Run export and compatibility tests**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/catalogue.test.ts src/dev/dev-server.test.ts src/testing/index.test.ts
```

Expected: PASS for export compatibility and existing `pushSchemaCatalogue` behavior.

- [ ] **Step 7: Commit schema/permissions push API**

```bash
git add packages/jazz-tools/src/dev/catalogue.ts packages/jazz-tools/src/dev/dev-server.ts packages/jazz-tools/src/dev/index.ts packages/jazz-tools/src/testing/local-jazz-server.ts packages/jazz-tools/src/dev/catalogue.test.ts packages/jazz-tools/src/dev/dev-server.test.ts
git commit -m "feat: expose dev catalogue push helpers"
```

---

### Task 3: Add Programmatic Push Result Tests

**Files:**

- Modify: `packages/jazz-tools/src/dev/catalogue.test.ts`

- [ ] **Step 1: Add fixture helpers using public APIs**

Append to `catalogue.test.ts`:

```ts
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, vi } from "vitest";

const tempRoots: string[] = [];
const APP_ID = "test-app";
const SERVER_URL = "http://localhost:1625";
const ADMIN_SECRET = "admin-secret";

afterEach(async () => {
  vi.unstubAllGlobals();
  await Promise.all(tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function createWorkspace(): Promise<{ root: string }> {
  const root = await mkdtemp(join(import.meta.dirname, ".catalogue-test-"));
  tempRoots.push(root);
  await mkdir(root, { recursive: true });
  await writeFile(join(root, "package.json"), '{ "type": "module" }\n');
  return { root };
}

function schemaSource(indexImportPath: string = "../index.ts"): string {
  return `
import { schema as s } from ${JSON.stringify(new URL(indexImportPath, import.meta.url).pathname)};

const schema = {
  todos: s.table({
    title: s.string(),
    ownerId: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`;
}

function permissionsSource(indexImportPath: string = "../index.ts"): string {
  return `
import { schema as s } from ${JSON.stringify(new URL(indexImportPath, import.meta.url).pathname)};
import { app } from "./schema.ts";

export default s.definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ ownerId: session.user_id }),
]);
`;
}
```

- [ ] **Step 2: Add `pushSchema` result test**

Append:

```ts
it("pushSchema publishes the local structural schema and returns a structured result", async () => {
  const { root } = await createWorkspace();
  await writeFile(join(root, "schema.ts"), schemaSource());

  let publishBody: any;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith(`/apps/${APP_ID}/admin/schemas`)) {
        publishBody = JSON.parse(String(init?.body));
        return new Response(
          JSON.stringify({
            objectId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            hash: "1234123412341234123412341234123412341234123412341234123412341234",
          }),
          { status: 201 },
        );
      }
      throw new Error(`Unexpected fetch: ${input}`);
    }),
  );

  const events: unknown[] = [];
  const { pushSchema } = await import("./index.js");
  const result = await pushSchema({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    adminSecret: ADMIN_SECRET,
    schemaDir: root,
    onEvent: (event) => events.push(event),
  });

  expect(result).toMatchObject({
    hash: "1234123412341234123412341234123412341234123412341234123412341234",
    status: "published",
    objectId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  });
  expect(publishBody.schema.todos.columns.map((column: any) => column.name)).toEqual([
    "title",
    "ownerId",
  ]);
  expect(events).toContainEqual({ type: "schema-loaded", schemaFile: join(root, "schema.ts") });
});
```

- [ ] **Step 3: Add `pushPermissions` result test**

Append:

```ts
it("pushPermissions publishes permissions against an explicit schema hash", async () => {
  const { root } = await createWorkspace();
  await writeFile(join(root, "schema.ts"), schemaSource());
  await writeFile(join(root, "permissions.ts"), permissionsSource());

  const schemaHash = "1234123412341234123412341234123412341234123412341234123412341234";
  const previousHead = {
    schemaHash,
    version: 2,
    parentBundleObjectId: "11111111-1111-1111-1111-111111111111",
    bundleObjectId: "22222222-2222-2222-2222-222222222222",
  };
  let permissionsBody: any;

  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith(`/apps/${APP_ID}/admin/permissions/head`)) {
        return new Response(JSON.stringify({ head: previousHead }), { status: 200 });
      }
      if (input.endsWith(`/apps/${APP_ID}/admin/permissions`)) {
        permissionsBody = JSON.parse(String(init?.body));
        return new Response(
          JSON.stringify({
            head: {
              schemaHash,
              version: 3,
              parentBundleObjectId: previousHead.bundleObjectId,
              bundleObjectId: "33333333-3333-3333-3333-333333333333",
            },
          }),
          { status: 201 },
        );
      }
      throw new Error(`Unexpected fetch: ${input}`);
    }),
  );

  const { pushPermissions } = await import("./index.js");
  const result = await pushPermissions({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    adminSecret: ADMIN_SECRET,
    schemaDir: root,
    schemaHash,
  });

  expect(result.previousHead).toEqual(previousHead);
  expect(result.head?.version).toBe(3);
  expect(permissionsBody.schemaHash).toBe(schemaHash);
  expect(permissionsBody.expectedParentBundleObjectId).toBe(previousHead.bundleObjectId);
  expect(Object.keys(permissionsBody.permissions)).toContain("todos");
});
```

- [ ] **Step 4: Run tests and commit**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/catalogue.test.ts
```

Expected: PASS.

Commit:

```bash
git add packages/jazz-tools/src/dev/catalogue.test.ts
git commit -m "test: cover dev schema and permissions push results"
```

---

### Task 4: Move Migration Push Logic Into Dev Catalogue

**Files:**

- Modify: `packages/jazz-tools/src/dev/catalogue.ts`
- Modify: `packages/jazz-tools/src/dev/index.ts`
- Modify: `packages/jazz-tools/src/cli.ts`
- Modify: `packages/jazz-tools/src/dev/catalogue.test.ts`
- Modify: `packages/jazz-tools/src/cli.test.ts`

- [ ] **Step 1: Add failing programmatic migration test**

Append to `catalogue.test.ts`:

```ts
it("pushMigration publishes a reviewed local migration file and returns a structured result", async () => {
  const { root } = await createWorkspace();
  const migrationsDir = join(root, "migrations");
  await mkdir(migrationsDir, { recursive: true });

  const fromHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const toHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  await writeFile(
    join(migrationsDir, `20260318-rename-${fromHash.slice(0, 12)}-${toHash.slice(0, 12)}.ts`),
    `
import { schema as s } from ${JSON.stringify(new URL("../index.ts", import.meta.url).pathname)};

export default s.defineMigration({
  migrate: {
    users: {
      email_address: s.renameFrom("email"),
    },
  },
  fromHash: ${JSON.stringify(fromHash.slice(0, 12))},
  toHash: ${JSON.stringify(toHash.slice(0, 12))},
  from: {
    users: s.table({
      email: s.string(),
    }),
  },
  to: {
    users: s.table({
      email_address: s.string(),
    }),
  },
});
`,
  );

  let migrationBody: any;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }
      if (input.endsWith(`/apps/${APP_ID}/admin/migrations`)) {
        migrationBody = JSON.parse(String(init?.body));
        return new Response(
          JSON.stringify({
            objectId: "44444444-4444-4444-4444-444444444444",
            fromHash,
            toHash,
          }),
          { status: 201 },
        );
      }
      throw new Error(`Unexpected fetch: ${input}`);
    }),
  );

  const { pushMigration } = await import("./index.js");
  const result = await pushMigration({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    adminSecret: ADMIN_SECRET,
    migrationsDir,
    fromHash: fromHash.slice(0, 12),
    toHash: toHash.slice(0, 12),
  });

  expect(result).toMatchObject({
    fromHash,
    toHash,
    status: "published",
    filePath: join(
      migrationsDir,
      `20260318-rename-${fromHash.slice(0, 12)}-${toHash.slice(0, 12)}.ts`,
    ),
  });
  expect(migrationBody.forward).toEqual([
    {
      table: "users",
      operations: [{ type: "rename", column: "email", value: "email_address" }],
    },
  ]);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/catalogue.test.ts
```

Expected: FAIL because `pushMigration` is not implemented/exported yet.

- [ ] **Step 3: Move migration helper logic from `cli.ts` to `dev/catalogue.ts`**

Move these helper groups from `cli.ts` into `catalogue.ts`:

- `SHORT_SCHEMA_HASH_LENGTH`
- `normalizeSchemaHashInput`
- `shortSchemaHash`
- `hashMatchesFullSchema`
- `resolveKnownSchemaHash`

Move the migration publication helpers with their existing bodies:

- `columnTypeSignature`
- `columnsEqual`
- `indexedColumnsEqual`
- `tableSchemasEqual`
- `tableSchemasRequireRowTransform`
- `wasmSchemasEqual`
- `schemaTransitionRequiresRowTransform`
- `sqlTypeToWasmColumnType`
- `serializeForwardLenses`
- `isDefinedMigration`
- `loadDefinedMigration`
- `unwrapMigrationExport`
- `findMigrationFile`

Also move the minimal snapshot/history helpers required for empty migration validation:

- `ResolvedSchemaInput`
- `looksLikeSnapshotFileName`
- `readSnapshotEntry`
- `listSnapshotEntries`
- `listSnapshotEntriesForMigrations`
- `snapshotFilename`
- `createTimestamp`
- `createSnapshotTimestampFromPublishedAt`
- `writeSnapshotSchemaForMigrations`
- `resolveHistoricalSchema`

Adapt `resolveHistoricalSchema` so its `appId`, `serverUrl`, and `adminSecret` parameters are required strings in `catalogue.ts`. Keep the CLI-only `requireAppId` and `requireSchemaExportServerValue` helpers in `cli.ts`.

Keep migration creation and migration stub rendering helpers in `cli.ts`.

- [ ] **Step 4: Implement programmatic `pushMigration`**

Add:

```ts
export interface PushMigrationOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  migrationsDir: string;
  fromHash: string;
  toHash: string;
  onEvent?: (event: CatalogueEvent) => void;
}

export interface PushMigrationResult {
  fromHash: string;
  toHash: string;
  status: "published";
  filePath?: string;
}

export async function pushMigration(options: PushMigrationOptions): Promise<PushMigrationResult> {
  const { hashes } = await fetchSchemaHashes(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });
  const fromHash = resolveKnownSchemaHash(options.fromHash, "fromHash", hashes);
  const toHash = resolveKnownSchemaHash(options.toHash, "toHash", hashes);

  let filePath: string | null = null;
  try {
    filePath = await findMigrationFile(options.migrationsDir, fromHash, toHash);
  } catch (error) {
    if (
      !(error instanceof Error) ||
      !error.message.startsWith(`No migration file found in ${options.migrationsDir}`)
    ) {
      throw error;
    }
  }

  if (!filePath) {
    const fromSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      fromHash,
      "fromHash",
      options.appId,
      options.serverUrl,
      options.adminSecret,
    );
    const toSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      toHash,
      "toHash",
      options.appId,
      options.serverUrl,
      options.adminSecret,
    );

    if (schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
      throw new Error(
        `No migration file found in ${options.migrationsDir} for ${fromHash} -> ${toHash}. Run \`jazz-tools migrations create ${options.appId} --fromHash ${shortSchemaHash(fromHash)} --toHash ${shortSchemaHash(toHash)}\` first.`,
      );
    }

    await publishStoredMigration(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      fromHash,
      toHash,
      forward: [],
    });

    emit(options, { type: "migration-published", fromHash, toHash });
    return { fromHash, toHash, status: "published" };
  }

  const migration = await loadDefinedMigration(filePath);
  if (
    !hashMatchesFullSchema(migration.fromHash, fromHash) ||
    !hashMatchesFullSchema(migration.toHash, toHash)
  ) {
    throw new Error(
      `Migration ${basename(filePath)} exports ${migration.fromHash} -> ${migration.toHash}, expected ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)}.`,
    );
  }

  schemaDefinitionToAst(migration.from as any);
  schemaDefinitionToAst(migration.to as any);

  if (migration.forward.length === 0) {
    const fromSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      fromHash,
      "fromHash",
      options.appId,
      options.serverUrl,
      options.adminSecret,
    );
    const toSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      toHash,
      "toHash",
      options.appId,
      options.serverUrl,
      options.adminSecret,
    );

    if (schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
      throw new Error(`Migration ${basename(filePath)} has no steps. Fill in migrate before push.`);
    }
  }

  await publishStoredMigration(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    fromHash,
    toHash,
    forward: migration.forward.length === 0 ? [] : serializeForwardLenses(migration.forward),
  });

  emit(options, { type: "migration-published", fromHash, toHash, filePath });
  return { fromHash, toHash, status: "published", filePath };
}
```

- [ ] **Step 5: Export migration API from `dev/index.ts`**

Add these names to the catalogue export block in `packages/jazz-tools/src/dev/index.ts`:

```ts
export { pushMigration, type PushMigrationOptions, type PushMigrationResult } from "./catalogue.js";
```

- [ ] **Step 6: Keep CLI `pushMigration` output wrapper**

Replace the current `cli.ts` exported `pushMigration` implementation with a wrapper:

```ts
import { pushMigration as pushMigrationProject } from "./dev/catalogue.js";

export async function pushMigration(options: PushMigrationOptions): Promise<void> {
  const { appId, serverUrl, adminSecret } = requireMigrationServerOptions(options);
  const result = await pushMigrationProject({
    appId,
    serverUrl,
    adminSecret,
    migrationsDir: options.migrationsDir,
    fromHash: options.fromHash,
    toHash: options.toHash,
  });

  if (result.filePath) {
    console.log(
      `Pushed migration ${shortSchemaHash(result.fromHash)} -> ${shortSchemaHash(result.toHash)} from ${basename(result.filePath)}.`,
    );
  } else {
    console.log(
      `Pushed migration ${shortSchemaHash(result.fromHash)} -> ${shortSchemaHash(result.toHash)} without a reviewed migration file because no row transformations are required.`,
    );
  }
}
```

If `shortSchemaHash` has moved to `catalogue.ts`, export it as an internal helper from that module and import it in `cli.ts`.

- [ ] **Step 7: Run migration tests**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/catalogue.test.ts src/cli.test.ts --testNamePattern "pushMigration|dev catalogue"
```

Expected: PASS for the new programmatic migration test and existing CLI migration tests.

- [ ] **Step 8: Commit migration move**

```bash
git add packages/jazz-tools/src/dev/catalogue.ts packages/jazz-tools/src/dev/index.ts packages/jazz-tools/src/cli.ts packages/jazz-tools/src/dev/catalogue.test.ts packages/jazz-tools/src/cli.test.ts
git commit -m "feat: expose dev migration push"
```

---

### Task 5: Move Deploy Orchestration Into Dev Catalogue

**Files:**

- Modify: `packages/jazz-tools/src/dev/catalogue.ts`
- Modify: `packages/jazz-tools/src/dev/index.ts`
- Modify: `packages/jazz-tools/src/cli.ts`
- Modify: `packages/jazz-tools/src/dev/catalogue.test.ts`
- Modify: `packages/jazz-tools/src/cli.test.ts`

- [ ] **Step 1: Add failing programmatic deploy tests**

Append to `catalogue.test.ts`:

```ts
it("deploy publishes schema and permissions and returns structured statuses", async () => {
  const { root } = await createWorkspace();
  await writeFile(join(root, "schema.ts"), schemaSource());
  await writeFile(join(root, "permissions.ts"), permissionsSource());

  const schemaHash = "1234123412341234123412341234123412341234123412341234123412341234";
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [] }), { status: 200 });
      }
      if (input.endsWith(`/apps/${APP_ID}/admin/schemas`)) {
        return new Response(
          JSON.stringify({ objectId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", hash: schemaHash }),
          { status: 201 },
        );
      }
      if (input.endsWith(`/apps/${APP_ID}/admin/permissions/head`)) {
        return new Response(JSON.stringify({ head: null }), { status: 200 });
      }
      if (input.endsWith(`/apps/${APP_ID}/admin/permissions`)) {
        const body = JSON.parse(String(init?.body));
        expect(body.schemaHash).toBe(schemaHash);
        return new Response(
          JSON.stringify({
            head: {
              schemaHash,
              version: 1,
              parentBundleObjectId: null,
              bundleObjectId: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            },
          }),
          { status: 201 },
        );
      }
      throw new Error(`Unexpected fetch: ${input}`);
    }),
  );

  const { deploy } = await import("./index.js");
  const result = await deploy({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    adminSecret: ADMIN_SECRET,
    schemaDir: root,
    migrationsDir: join(root, "migrations"),
  });

  expect(result.schema).toMatchObject({ hash: schemaHash, status: "published" });
  expect(result.permissions?.schemaHash).toBe(schemaHash);
  expect(result.warnings.length).toBeGreaterThan(0);
});

it("deploy returns schema-only status when permissions.ts is missing", async () => {
  const { root } = await createWorkspace();
  await writeFile(join(root, "schema.ts"), schemaSource());

  const schemaHash = "9999999999999999999999999999999999999999999999999999999999999999";
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string) => {
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [] }), { status: 200 });
      }
      if (input.endsWith(`/apps/${APP_ID}/admin/schemas`)) {
        return new Response(
          JSON.stringify({ objectId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", hash: schemaHash }),
          { status: 201 },
        );
      }
      if (input.includes(`/admin/permissions`)) {
        throw new Error("schema-only deploy should not publish permissions");
      }
      throw new Error(`Unexpected fetch: ${input}`);
    }),
  );

  const { deploy } = await import("./index.js");
  const result = await deploy({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    adminSecret: ADMIN_SECRET,
    schemaDir: root,
    migrationsDir: join(root, "migrations"),
  });

  expect(result.schema.hash).toBe(schemaHash);
  expect(result.permissions).toBeUndefined();
});
```

- [ ] **Step 2: Run deploy tests to verify failure**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/catalogue.test.ts --testNamePattern "deploy"
```

Expected: FAIL because programmatic `deploy` is not implemented yet.

- [ ] **Step 3: Add deploy result types**

Add to `catalogue.ts`:

```ts
export interface DeployOptions extends CatalogueProjectOptions {
  migrationsDir: string;
  noVerify?: boolean;
}

export interface DeployResult {
  schema: PushSchemaResult;
  migration?:
    | PushMigrationResult
    | { status: "already-connected"; fromHash: string; toHash: string };
  permissions?: PushPermissionsResult;
  warnings: string[];
}
```

- [ ] **Step 4: Implement stored schema lookup helpers**

Move/adapt from `cli.ts`:

```ts
async function resolveStoredStructuralSchemaHash(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<string | null> {
  const { hashes } = await fetchSchemaHashes(serverUrl, { appId, adminSecret });
  const storedSchemas = await Promise.all(
    hashes.map(async (hash) => ({
      hash,
      schema: (await fetchStoredWasmSchema(serverUrl, { appId, adminSecret, schemaHash: hash }))
        .schema,
    })),
  );

  return storedSchemas.find(({ schema }) => wasmSchemasEqual(schema, wasmSchema))?.hash ?? null;
}
```

- [ ] **Step 5: Implement programmatic `deploy`**

Add:

```ts
export async function deploy(options: DeployOptions): Promise<DeployResult> {
  const warnings: string[] = [];
  const compiled = await loadCompiledSchema(options.schemaDir);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });

  for (const diagnostic of collectMissingExplicitPolicyDiagnostics(
    compiled.schema.tables.map((table) => table.name),
    compiled.permissions,
  )) {
    warnings.push(diagnostic.message);
    emit(options, { type: "warning", message: diagnostic.message });
  }

  let schemaHash = await resolveStoredStructuralSchemaHash(
    options.appId,
    options.serverUrl,
    options.adminSecret,
    compiled.wasmSchema,
  );

  let schemaResult: PushSchemaResult;
  if (!schemaHash) {
    const published = await publishStoredSchema(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schema: compiled.wasmSchema,
    });
    schemaHash = published.hash;
    emit(options, { type: "schema-published", hash: schemaHash, objectId: published.objectId });
    schemaResult = {
      hash: schemaHash,
      schemaFile: compiled.schemaFile,
      status: "published",
      objectId: published.objectId,
    };
  } else {
    emit(options, { type: "schema-skipped", hash: schemaHash, reason: "already-stored" });
    schemaResult = {
      hash: schemaHash,
      schemaFile: compiled.schemaFile,
      status: "already-stored",
    };
  }

  if (!compiled.permissions || !compiled.permissionsFile) {
    emit(options, { type: "permissions-skipped", reason: "missing-permissions-file" });
    return { schema: schemaResult, warnings };
  }

  emit(options, { type: "permissions-loaded", permissionsFile: compiled.permissionsFile });
  const { head: currentHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  let migration: DeployResult["migration"];
  if (currentHead && currentHead.schemaHash !== schemaHash) {
    const { connected } = await fetchSchemaConnectivity(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      fromHash: currentHead.schemaHash,
      toHash: schemaHash,
    });

    if (connected) {
      migration = {
        status: "already-connected",
        fromHash: currentHead.schemaHash,
        toHash: schemaHash,
      };
    } else {
      try {
        migration = await pushMigration({
          appId: options.appId,
          serverUrl: options.serverUrl,
          adminSecret: options.adminSecret,
          migrationsDir: options.migrationsDir,
          fromHash: currentHead.schemaHash,
          toHash: schemaHash,
          onEvent: options.onEvent,
        });
      } catch (error) {
        const migrationMissingPrefix = `No migration file found in ${options.migrationsDir}`;
        if (!(error instanceof Error) || !error.message.startsWith(migrationMissingPrefix)) {
          throw error;
        }

        const message = `The new permissions schema ${shortSchemaHash(schemaHash)} is not connected to the previous permissions schema ${shortSchemaHash(currentHead.schemaHash)} on the server. Reads and writes may fail until you push a migration. Run \`jazz-tools migrations create ${options.appId} --fromHash ${shortSchemaHash(currentHead.schemaHash)} --toHash ${shortSchemaHash(schemaHash)}\` to create a migration and then re-run this command.`;
        if (options.noVerify) {
          warnings.push(message);
          emit(options, { type: "warning", message });
        } else {
          throw new Error(message);
        }
      }
    }
  }

  const { head } = await publishStoredPermissions(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash,
    permissions: compiled.permissions,
    expectedParentBundleObjectId: currentHead?.bundleObjectId ?? null,
  });

  emit(options, { type: "permissions-published", schemaHash, version: head?.version });
  return {
    schema: schemaResult,
    migration,
    permissions: {
      schemaHash,
      permissionsFile: compiled.permissionsFile,
      previousHead: currentHead,
      head,
    },
    warnings,
  };
}
```

- [ ] **Step 6: Export deploy API from `dev/index.ts`**

Add these names to the catalogue export block in `packages/jazz-tools/src/dev/index.ts`:

```ts
export { deploy, type DeployOptions, type DeployResult } from "./catalogue.js";
```

- [ ] **Step 7: Keep CLI `deploy` wrapper messages**

Replace current CLI `deploy` body with:

```ts
import { deploy as deployProject, type CatalogueEvent } from "./dev/catalogue.js";

function logDeployEvent(event: CatalogueEvent): void {
  switch (event.type) {
    case "schema-loaded":
      console.log(`Loaded current schema from ${event.schemaFile}.`);
      break;
    case "schema-published":
      console.log(`Published the current schema as ${shortSchemaHash(event.hash)}.`);
      break;
    case "schema-skipped":
      console.log(
        `The current schema is already stored in the server as ${shortSchemaHash(event.hash)}; skipping publish.`,
      );
      break;
    case "permissions-loaded":
      console.log(`Loaded current permissions from ${event.permissionsFile}.`);
      break;
    case "permissions-skipped":
      console.log("No permissions.ts found; skipping permissions publish.");
      break;
    case "permissions-published":
      console.log(`Published permissions against ${shortSchemaHash(event.schemaHash)}.`);
      break;
    case "migration-published":
      if (event.filePath) {
        console.log(
          `Pushed migration ${shortSchemaHash(event.fromHash)} -> ${shortSchemaHash(event.toHash)} from ${basename(event.filePath)}.`,
        );
      } else {
        console.log(
          `Pushed migration ${shortSchemaHash(event.fromHash)} -> ${shortSchemaHash(event.toHash)} without a reviewed migration file because no row transformations are required.`,
        );
      }
      break;
    case "warning":
      console.warn(`\x1b[33m${event.message}\x1b[0m`);
      break;
  }
}

export async function deploy(options: DeployOptions): Promise<void> {
  await deployProject({
    ...options,
    onEvent: logDeployEvent,
  });
}
```

If existing CLI tests require `Published permissions as vN on HASH.`, keep that exact final message by formatting the returned `result.permissions.head` after `deployProject(...)` instead of using only the event.

- [ ] **Step 8: Run deploy-focused tests**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/catalogue.test.ts src/cli.test.ts --testNamePattern "deploy|dev catalogue"
```

Expected: PASS. Existing CLI deploy message assertions must still pass.

- [ ] **Step 9: Commit deploy move**

```bash
git add packages/jazz-tools/src/dev/catalogue.ts packages/jazz-tools/src/dev/index.ts packages/jazz-tools/src/cli.ts packages/jazz-tools/src/dev/catalogue.test.ts packages/jazz-tools/src/cli.test.ts
git commit -m "feat: expose programmatic dev deploy"
```

---

### Task 6: Full Verification And Documentation Touches

**Files:**

- No documentation file changes are planned in this task. The public user docs already describe CLI deploy behavior, and this change adds a programmatic `jazz-tools/dev` surface without changing the documented command flow.

- [ ] **Step 1: Run focused package tests**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/dev/catalogue.test.ts src/dev/dev-server.test.ts src/testing/index.test.ts src/cli.test.ts
```

Expected: PASS.

- [ ] **Step 2: Run type/build verification**

Run:

```bash
pnpm --filter jazz-tools build:runtime
```

Expected: PASS with TypeScript declaration output generated under `packages/jazz-tools/dist`.

- [ ] **Step 3: Inspect public export declarations**

Run:

```bash
rg -n "pushSchema|pushPermissions|pushMigration|deploy|pushSchemaCatalogue" packages/jazz-tools/dist/dev/index.d.ts packages/jazz-tools/dist/dev/catalogue.d.ts
```

Expected: declarations include the new public API names and compatibility `pushSchemaCatalogue`.

- [ ] **Step 4: Run repository-level relevant build**

Run:

```bash
pnpm build:core
```

Expected: PASS.

- [ ] **Step 5: Final git diff review**

Run:

```bash
git status --short
git diff --stat HEAD
git diff HEAD -- packages/jazz-tools/src/dev/catalogue.ts packages/jazz-tools/src/cli.ts packages/jazz-tools/src/dev/index.ts packages/jazz-tools/src/dev/dev-server.ts packages/jazz-tools/src/testing/local-jazz-server.ts
```

Expected: only intended implementation, tests, and optional docs are changed.

- [ ] **Step 6: Commit verification cleanup**

If verification produces formatting or small cleanup changes:

```bash
git add packages/jazz-tools/src/dev/catalogue.ts packages/jazz-tools/src/cli.ts packages/jazz-tools/src/dev/index.ts packages/jazz-tools/src/dev/dev-server.ts packages/jazz-tools/src/testing/local-jazz-server.ts packages/jazz-tools/src/dev/catalogue.test.ts packages/jazz-tools/src/dev/dev-server.test.ts packages/jazz-tools/src/cli.test.ts
git commit -m "chore: finalize dev catalogue API"
```

If no changes remain after verification, do not create an empty commit.

---

## Self-Review

- Spec coverage:
  - `jazz-tools/dev` public API is covered in Tasks 1, 2, 4, and 5.
  - Single schema/permissions/migration push functions are covered in Tasks 2, 3, and 4.
  - Programmatic `deploy` is covered in Task 5.
  - CLI wrapper preservation is covered in Tasks 4 and 5.
  - `pushSchemaCatalogue` compatibility and dev auto-push separation are covered in Tasks 1 and 2.
  - Black-box tests using public schema/permissions APIs are covered in Tasks 3, 4, and 5.
- Placeholder scan:
  - No placeholder markers or open-ended test-writing steps remain.
  - Code-moving steps name exact existing helper groups and exact target files.
- Type consistency:
  - `CatalogueEvent`, `PushSchemaOptions`, `PushPermissionsOptions`, `PushMigrationOptions`, and `DeployOptions` are introduced before dependent tasks consume them.
  - CLI wrappers keep the existing `Promise<void>` behavior for current CLI tests while dev functions return structured results.
