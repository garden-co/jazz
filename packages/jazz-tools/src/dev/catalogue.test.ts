import { mkdir, mkdtemp, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { build as esbuild, type BuildOptions } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import { createNapiNativeRuntimeAdapter } from "../runtime/testing/napi-runtime-test-utils.js";
import { deploy as deployCatalogue } from "./catalogue.js";
import { legacyByteaStructuralSchemaHash, structuralSchemaHash } from "./schema-utils.js";

const tempRoots: string[] = [];
const APP_ID = "test-app";
const SERVER_URL = "http://localhost:1625";
const ADMIN_SECRET = "admin-secret";
const SCHEMA_HASH = "1234123412341234123412341234123412341234123412341234123412341234";
const SCHEMA_OBJECT_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

afterEach(async () => {
  vi.unstubAllGlobals();
  await Promise.all(tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function createWorkspace(): Promise<{ root: string }> {
  // The browser suite verifies artifact provenance while the Node suite runs.
  // A checkout-local test workspace would be an untracked input midway through
  // that verification, so keep this fixture out of the repository.
  const root = await mkdtemp(join(tmpdir(), "jazz-tools-catalogue-test-"));
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
  policy.todos.allowRead.where({ ownerId: session.user }),
]);
`;
}

describe("dev catalogue API exports", () => {
  it("exports catalogue operations from jazz-tools/dev", async () => {
    const dev = await import("./index.js");

    expect(typeof dev.pushSchema).toBe("function");
    expect(typeof dev.pushPermissions).toBe("function");
    expect(typeof dev.pushMigration).toBe("function");
    expect(typeof dev.deploy).toBe("function");
  });

  // These public entrypoints load the native dev-server module transitively.
  // The assertion is import identity, so give that one-time native module
  // initialization a lifecycle budget without relaxing catalogue operations.
  it("keeps deploy compatible across dev and testing entrypoints", async () => {
    const dev = await import("./index.js");
    const testing = await import("../testing/index.js");

    expect(testing.deploy).toBe(dev.deploy);
  }, 15_000);
});

describe("dev catalogue runtime schema identity", () => {
  it("opens a NativeRuntimeAdapter for representative public schema shapes", async () => {
    const schema = {
      users: s.table({
        name: s.string(),
      }),
      files: s.table({
        ownerId: s.ref("users"),
        contents: s.bytes().default(new Uint8Array([0, 1, 127, 255])),
        mediaType: s.enum("image/png", "text/plain").default("text/plain"),
        tags: s.array(s.string()).default(["draft", "review"]),
      }),
      comments: s
        .table({
          fileId: s.ref("files"),
          authorId: s.ref("users").optional().default(null),
          body: s.string(),
          attachmentIds: s.array(s.ref("files")).default([]),
          status: s.enum("open", "resolved").default("open"),
        })
        .indexOnly(["fileId", "status"]),
    };
    const app = s.defineApp(schema);
    await createNapiNativeRuntimeAdapter(app.wasmSchema);

    expect(serializeRuntimeSchema(app.wasmSchema)).toContain("__jazzRuntimeSchema");
  });
});

describe("legacy Bytea catalogue identity", () => {
  it("publishes the corrected identity and connects the legacy identity with a migration", async () => {
    const schema = s.defineApp({
      files: s.table({
        payload: s.bytes(),
      }),
    }).wasmSchema;
    const legacyHash = legacyByteaStructuralSchemaHash(schema);
    const currentHash = structuralSchemaHash(schema);
    const previousHead = {
      schemaHash: legacyHash,
      version: 1,
      parentBundleObjectId: null,
      bundleObjectId: "11111111-1111-1111-1111-111111111111",
    };
    const nextHead = {
      schemaHash: currentHash,
      version: 2,
      parentBundleObjectId: previousHead.bundleObjectId,
      bundleObjectId: "22222222-2222-2222-2222-222222222222",
    };
    let migrationBody: Record<string, unknown> | undefined;

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [legacyHash] }), { status: 200 });
        }
        if (
          url.endsWith(`/apps/${APP_ID}/schema/${legacyHash}`) ||
          url.endsWith(`/apps/${APP_ID}/schema/${currentHash}`)
        ) {
          return new Response(JSON.stringify({ schema: { tables: schema }, publishedAt: 1 }), {
            status: 200,
          });
        }
        if (url.endsWith(`/apps/${APP_ID}/admin/schemas`)) {
          return new Response(
            JSON.stringify({
              objectId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
              hash: currentHash,
            }),
            { status: 201 },
          );
        }
        if (url.endsWith(`/apps/${APP_ID}/admin/permissions/head`)) {
          return new Response(JSON.stringify({ head: previousHead }), { status: 200 });
        }
        if (url.includes(`/apps/${APP_ID}/admin/schema-connectivity`)) {
          return new Response(JSON.stringify({ connected: false }), { status: 200 });
        }
        if (url.endsWith(`/apps/${APP_ID}/admin/migrations`)) {
          migrationBody = JSON.parse(String(init?.body));
          return new Response(
            JSON.stringify({
              objectId: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
              fromHash: legacyHash,
              toHash: currentHash,
            }),
            { status: 201 },
          );
        }
        if (url.endsWith(`/apps/${APP_ID}/admin/permissions`)) {
          return new Response(JSON.stringify({ head: nextHead }), { status: 201 });
        }
        throw new Error(`Unexpected fetch: ${url}`);
      }),
    );
    const result = await deployCatalogue({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schema,
      permissions: {},
    });

    expect(legacyHash).not.toBe(currentHash);
    expect(result.schema).toMatchObject({ hash: currentHash, status: "published" });
    expect(result.migration).toMatchObject({
      fromHash: legacyHash,
      toHash: currentHash,
      status: "published",
    });
    expect(migrationBody).toEqual({
      fromHash: legacyHash,
      toHash: currentHash,
      forward: [],
    });
    expect(result.permissions?.schemaHash).toBe(currentHash);
  });

  it("connects the durable legacy identity when deploying without permissions", async () => {
    const schema = s.defineApp({
      files: s.table({
        payload: s.bytes(),
      }),
    }).wasmSchema;
    const legacyHash = legacyByteaStructuralSchemaHash(schema);
    const currentHash = structuralSchemaHash(schema);
    let migrationBody: Record<string, unknown> | undefined;

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [legacyHash] }), { status: 200 });
        }
        if (
          url.endsWith(`/apps/${APP_ID}/schema/${legacyHash}`) ||
          url.endsWith(`/apps/${APP_ID}/schema/${currentHash}`)
        ) {
          return new Response(JSON.stringify({ schema: { tables: schema }, publishedAt: 1 }), {
            status: 200,
          });
        }
        if (url.endsWith(`/apps/${APP_ID}/admin/schemas`)) {
          return new Response(
            JSON.stringify({
              objectId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
              hash: currentHash,
            }),
            { status: 201 },
          );
        }
        if (url.includes(`/apps/${APP_ID}/admin/schema-connectivity`)) {
          return new Response(JSON.stringify({ connected: false }), { status: 200 });
        }
        if (url.endsWith(`/apps/${APP_ID}/admin/migrations`)) {
          migrationBody = JSON.parse(String(init?.body));
          return new Response(
            JSON.stringify({
              objectId: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
              fromHash: legacyHash,
              toHash: currentHash,
            }),
            { status: 201 },
          );
        }
        throw new Error(`Unexpected fetch: ${url}`);
      }),
    );

    const result = await deployCatalogue({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schema,
    });

    expect(legacyHash).not.toBe(currentHash);
    expect(result.schema).toMatchObject({ hash: currentHash, status: "published" });
    expect(result.migration).toMatchObject({
      fromHash: legacyHash,
      toHash: currentHash,
      status: "published",
    });
    expect(migrationBody).toEqual({
      fromHash: legacyHash,
      toHash: currentHash,
      forward: [],
    });
    expect(result.permissions).toBeUndefined();
  });
});

describe("dev catalogue push behavior", () => {
  it("deploy publishes schema and permissions and returns structured statuses", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), schemaSource());
    await writeFile(join(root, "permissions.ts"), permissionsSource());

    const permissionsHead = {
      schemaHash: SCHEMA_HASH,
      version: 1,
      parentBundleObjectId: null,
      bundleObjectId: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    };
    let schemaPublishBody: any;
    let permissionsPublishBody: any;

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init?: RequestInit) => {
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [] }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/schemas`)) {
          schemaPublishBody = JSON.parse(String(init?.body));
          return new Response(
            JSON.stringify({
              objectId: SCHEMA_OBJECT_ID,
              hash: SCHEMA_HASH,
            }),
            { status: 201 },
          );
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/permissions/head`)) {
          return new Response(JSON.stringify({ head: null }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/permissions`)) {
          permissionsPublishBody = JSON.parse(String(init?.body));
          return new Response(JSON.stringify({ head: permissionsHead }), { status: 201 });
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const events: unknown[] = [];
    const { deploy } = await import("./catalogue-project.js");
    const result = await deploy({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schemaDir: root,
      onEvent: (event) => events.push(event),
    });

    expect(result.schema).toEqual({
      hash: SCHEMA_HASH,
      schemaFile: join(root, "schema.ts"),
      status: "published",
      objectId: SCHEMA_OBJECT_ID,
    });
    expect(result.permissions).toEqual({
      schemaHash: SCHEMA_HASH,
      permissionsFile: join(root, "permissions.ts"),
      previousHead: null,
      head: permissionsHead,
    });
    expect(result.migration).toBeUndefined();
    expect(result.warnings).toContain(
      'Warning: table "todos" has a policy set but no explicit insert policy in permissions.ts; inserts will be denied.',
    );
    expect(schemaPublishBody.schema.tables.todos.columns.map((column: any) => column.name)).toEqual(
      ["title", "ownerId"],
    );
    expect(permissionsPublishBody.schemaHash).toBe(SCHEMA_HASH);
    expect(permissionsPublishBody.expectedParentBundleObjectId).toBeNull();
    expect(Object.keys(permissionsPublishBody.permissions)).toContain("todos");
    expect(events).toContainEqual({ type: "schema-loaded", schemaFile: join(root, "schema.ts") });
    expect(events).toContainEqual({
      type: "schema-published",
      hash: SCHEMA_HASH,
      objectId: SCHEMA_OBJECT_ID,
    });
    expect(events).toContainEqual({
      type: "warning",
      message:
        'Warning: table "todos" has a policy set but no explicit insert policy in permissions.ts; inserts will be denied.',
    });
    expect(events).toContainEqual({
      type: "permissions-loaded",
      permissionsFile: join(root, "permissions.ts"),
    });
    expect(events).toContainEqual({
      type: "permissions-published",
      schemaHash: SCHEMA_HASH,
      version: 1,
    });
  });

  it("deploy returns schema-only status when permissions.ts is missing", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), schemaSource());

    const storedHash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const storedSchema = s.defineApp({
      todos: s.table({
        title: s.string(),
        ownerId: s.string(),
      }),
    }).wasmSchema;

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string) => {
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [storedHash] }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/schema/${storedHash}`)) {
          return new Response(
            JSON.stringify({ schema: { tables: storedSchema }, publishedAt: 0 }),
            {
              status: 200,
            },
          );
        }
        if (input.includes(`/admin/permissions`) || input.endsWith(`/admin/schemas`)) {
          throw new Error("deploy() should not publish when schema is already stored.");
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const events: unknown[] = [];
    const { deploy } = await import("./catalogue-project.js");
    const result = await deploy({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schemaDir: root,
      onEvent: (event) => events.push(event),
    });

    expect(result).toEqual({
      schema: {
        hash: storedHash,
        schemaFile: join(root, "schema.ts"),
        status: "already-stored",
      },
      warnings: [
        'Warning: table "todos" has no policy declarations in permissions.ts; it remains open for reads, inserts, updates, and deletes until its first policy is declared.',
      ],
    });
    expect(events).toContainEqual({
      type: "schema-skipped",
      hash: storedHash,
      reason: "already-stored",
    });
    expect(events).toContainEqual({
      type: "permissions-skipped",
      reason: "missing-permissions-file",
    });
  });

  it("deploy reports an already-connected migration when retargeting connected schemas", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), schemaSource());
    await writeFile(join(root, "permissions.ts"), permissionsSource());

    const previousSchemaHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const nextSchemaHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const storedSchema = s.defineApp({
      todos: s.table({
        title: s.string(),
        ownerId: s.string(),
      }),
    }).wasmSchema;
    const previousHead = {
      schemaHash: previousSchemaHash,
      version: 4,
      parentBundleObjectId: "11111111-1111-1111-1111-111111111111",
      bundleObjectId: "22222222-2222-2222-2222-222222222222",
    };
    const nextHead = {
      schemaHash: nextSchemaHash,
      version: 5,
      parentBundleObjectId: previousHead.bundleObjectId,
      bundleObjectId: "33333333-3333-3333-3333-333333333333",
    };

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init?: RequestInit) => {
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [nextSchemaHash] }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/schema/${nextSchemaHash}`)) {
          return new Response(
            JSON.stringify({ schema: { tables: storedSchema }, publishedAt: 0 }),
            {
              status: 200,
            },
          );
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/permissions/head`)) {
          return new Response(JSON.stringify({ head: previousHead }), { status: 200 });
        }
        if (input.includes(`/apps/${APP_ID}/admin/schema-connectivity?`)) {
          const url = new URL(input);
          expect(url.searchParams.get("fromHash")).toBe(previousSchemaHash);
          expect(url.searchParams.get("toHash")).toBe(nextSchemaHash);
          return new Response(JSON.stringify({ connected: true }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/permissions`)) {
          const body = JSON.parse(String(init?.body));
          expect(body.schemaHash).toBe(nextSchemaHash);
          expect(body.expectedParentBundleObjectId).toBe(previousHead.bundleObjectId);
          return new Response(JSON.stringify({ head: nextHead }), { status: 201 });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/migrations`)) {
          throw new Error("deploy() should not push a migration when schemas are connected.");
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const events: unknown[] = [];
    const { deploy } = await import("./catalogue-project.js");
    const result = await deploy({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schemaDir: root,
      onEvent: (event) => events.push(event),
    });

    expect(result.migration).toEqual({
      status: "already-connected",
      fromHash: previousSchemaHash,
      toHash: nextSchemaHash,
    });
    expect(result.permissions?.previousHead).toEqual(previousHead);
    expect(result.permissions?.head).toEqual(nextHead);
    expect(events).toContainEqual({
      type: "migration-skipped",
      reason: "already-connected",
      fromHash: previousSchemaHash,
      toHash: nextSchemaHash,
    });
  });

  it("pushMigration publishes an inferred empty migration and emits a catalogue event", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await mkdir(migrationsDir, { recursive: true });

    const fromHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const toHash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const objectId = "55555555-5555-5555-5555-555555555555";
    const fromSchema = s.defineApp({
      todos: s.table({
        title: s.string(),
        done: s.boolean(),
      }),
    }).wasmSchema;
    const toSchema = s.defineApp({
      todos: s.table({
        done: s.boolean(),
        title: s.string(),
      }),
    }).wasmSchema;

    let migrationBody: any;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init?: RequestInit) => {
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
          return new Response(JSON.stringify({ schema: { tables: fromSchema }, publishedAt: 0 }), {
            status: 200,
          });
        }
        if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
          return new Response(JSON.stringify({ schema: { tables: toSchema }, publishedAt: 0 }), {
            status: 200,
          });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/migrations`)) {
          migrationBody = JSON.parse(String(init?.body));
          return new Response(JSON.stringify({ objectId, fromHash, toHash }), { status: 201 });
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const events: unknown[] = [];
    const { pushMigration } = await import("./catalogue-project.js");
    const result = await pushMigration({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      migrationsDir,
      fromHash: fromHash.slice(0, 12),
      toHash: toHash.slice(0, 12),
      onEvent: (event) => events.push(event),
    });

    expect(result).toEqual({
      fromHash,
      toHash,
      status: "published",
      objectId,
    });
    expect(migrationBody.forward).toEqual([]);
    expect(events).toEqual([{ type: "migration-published", fromHash, toHash }]);
  });

  it("pushMigration publishes a reviewed local migration file and returns a structured result", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await mkdir(migrationsDir, { recursive: true });

    const fromSchema = {
      users: s.table({
        email: s.string(),
      }),
    };
    const toSchema = {
      users: s.table({
        email_address: s.string(),
      }),
    };
    const { computeSchemaHash } = await import("./catalogue.js");
    const fromHash = await computeSchemaHash(s.defineApp(fromSchema).wasmSchema);
    const toHash = await computeSchemaHash(s.defineApp(toSchema).wasmSchema);

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
        if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
          return new Response(
            JSON.stringify({
              schema: { tables: s.defineApp(fromSchema).wasmSchema },
              publishedAt: 0,
            }),
            { status: 200 },
          );
        }
        if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
          return new Response(
            JSON.stringify({
              schema: { tables: s.defineApp(toSchema).wasmSchema },
              publishedAt: 0,
            }),
            { status: 200 },
          );
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

    const { pushMigration } = await import("./catalogue-project.js");
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

  it("pushMigration refuses a symlinked executable migration before publication", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await mkdir(migrationsDir, { recursive: true });
    const fromHash = "c".repeat(64);
    const toHash = "d".repeat(64);
    const migrationName = `20260318-rename-${fromHash.slice(0, 12)}-${toHash.slice(0, 12)}.ts`;
    const outsideMigration = join(root, "outside-migration.ts");
    await writeFile(outsideMigration, "export default {};\n");
    await symlink(outsideMigration, join(migrationsDir, migrationName), "file");

    let publicationAttempts = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string) => {
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/migrations`)) {
          publicationAttempts++;
          return new Response("unexpected", { status: 500 });
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const { pushMigration } = await import("./catalogue-project.js");
    await expect(
      pushMigration({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        migrationsDir,
        fromHash,
        toHash,
      }),
    ).rejects.toThrow("Migration path must not contain a symlink or junction");
    expect(publicationAttempts).toBe(0);
  });

  it("pushMigration refuses a symlinked migrations directory before contacting the server", async () => {
    const { root } = await createWorkspace();
    const outsideMigrations = join(root, "outside-migrations");
    const migrationsDir = join(root, "migrations");
    await mkdir(outsideMigrations, { recursive: true });
    await symlink(outsideMigrations, migrationsDir, "dir");

    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { pushMigration } = await import("./catalogue-project.js");
    await expect(
      pushMigration({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        migrationsDir,
        fromHash: "c".repeat(64),
        toHash: "d".repeat(64),
      }),
    ).rejects.toThrow("Migration path must not contain a symlink or junction");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each(["hash", "witness"] as const)(
    "rejects a migration whose embedded %s metadata was tampered before publication",
    async (tamperKind) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      await mkdir(migrationsDir, { recursive: true });

      const fromSchema = {
        users: s.table({
          email: s.string(),
        }),
      };
      const toSchema = {
        users: s.table({
          email_address: s.string(),
        }),
      };
      const { computeSchemaHash } = await import("./catalogue.js");
      const fromWasmSchema = s.defineApp(fromSchema).wasmSchema;
      const toWasmSchema = s.defineApp(toSchema).wasmSchema;
      const fromHash = await computeSchemaHash(fromWasmSchema);
      const toHash = await computeSchemaHash(toWasmSchema);
      const embeddedFromHash = tamperKind === "hash" ? "eeeeeeeeeeee" : fromHash.slice(0, 12);
      const fromColumn = tamperKind === "witness" ? "s.int()" : "s.string()";

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
  fromHash: ${JSON.stringify(embeddedFromHash)},
  toHash: ${JSON.stringify(toHash.slice(0, 12))},
  from: {
    users: s.table({
      email: ${fromColumn},
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

      let migrationPublishCount = 0;
      vi.stubGlobal(
        "fetch",
        vi.fn(async (input: string) => {
          if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
            return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
          }
          if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
            return new Response(
              JSON.stringify({ schema: { tables: fromWasmSchema }, publishedAt: 0 }),
              { status: 200 },
            );
          }
          if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
            return new Response(
              JSON.stringify({ schema: { tables: toWasmSchema }, publishedAt: 0 }),
              { status: 200 },
            );
          }
          if (input.endsWith(`/apps/${APP_ID}/admin/migrations`)) {
            migrationPublishCount++;
            return new Response(JSON.stringify({ fromHash, toHash }), { status: 201 });
          }
          throw new Error(`Unexpected fetch: ${input}`);
        }),
      );

      const { pushMigration } = await import("./catalogue-project.js");
      const expectedError = tamperKind === "hash" ? "embedded fromHash" : "from schema witness";
      await expect(
        pushMigration({
          appId: APP_ID,
          serverUrl: SERVER_URL,
          adminSecret: ADMIN_SECRET,
          migrationsDir,
          fromHash,
          toHash,
        }),
      ).rejects.toThrow(expectedError);
      expect(migrationPublishCount).toBe(0);
      const temporaryEntries = (await readdir(migrationsDir)).filter((entry) =>
        entry.startsWith(".jazz-bundle-"),
      );
      expect(temporaryEntries).toEqual([]);
    },
  );

  it("uses private migration bundles across isolated process realms and removes them", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await mkdir(migrationsDir, { recursive: true });

    const fromSchema = {
      users: s.table({
        email: s.string(),
      }),
    };
    const toSchema = {
      users: s.table({
        email_address: s.string(),
      }),
    };
    const { computeSchemaHash } = await import("./catalogue.js");
    const fromWasmSchema = s.defineApp(fromSchema).wasmSchema;
    const toWasmSchema = s.defineApp(toSchema).wasmSchema;
    const fromHash = await computeSchemaHash(fromWasmSchema);
    const toHash = await computeSchemaHash(toWasmSchema);
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

    const activeOutputs = new Set<string>();
    let arrivals = 0;
    let releaseBuilds!: () => void;
    const bothBuildsStarted = new Promise<void>((resolve) => {
      releaseBuilds = resolve;
    });
    vi.doMock("esbuild", () => ({
      build: async (options: BuildOptions) => {
        const outFile = options.outfile!;
        arrivals++;
        if (activeOutputs.has(outFile)) {
          releaseBuilds();
          throw new Error(`migration bundle collision: ${outFile}`);
        }
        activeOutputs.add(outFile);
        if (arrivals === 2) releaseBuilds();
        await bothBuildsStarted;
        try {
          return await esbuild(options);
        } finally {
          activeOutputs.delete(outFile);
        }
      },
    }));

    let migrationPublishCount = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string) => {
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
          return new Response(
            JSON.stringify({ schema: { tables: fromWasmSchema }, publishedAt: 0 }),
            { status: 200 },
          );
        }
        if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
          return new Response(
            JSON.stringify({ schema: { tables: toWasmSchema }, publishedAt: 0 }),
            { status: 200 },
          );
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/migrations`)) {
          migrationPublishCount++;
          return new Response(JSON.stringify({ fromHash, toHash }), { status: 201 });
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    try {
      // Fresh module evaluations model two processes whose module-local counters both start at zero.
      vi.resetModules();
      const firstProcess = await import("./catalogue-project.js");
      vi.resetModules();
      const secondProcess = await import("./catalogue-project.js");

      const results = await Promise.all([
        firstProcess.pushMigration({
          appId: APP_ID,
          serverUrl: SERVER_URL,
          adminSecret: ADMIN_SECRET,
          migrationsDir,
          fromHash,
          toHash,
        }),
        secondProcess.pushMigration({
          appId: APP_ID,
          serverUrl: SERVER_URL,
          adminSecret: ADMIN_SECRET,
          migrationsDir,
          fromHash,
          toHash,
        }),
      ]);

      expect(results).toHaveLength(2);
      expect(migrationPublishCount).toBe(2);
      expect(
        (await readdir(migrationsDir)).filter((entry) => entry.startsWith(".jazz-bundle-")),
      ).toEqual([]);
    } finally {
      vi.doUnmock("esbuild");
      vi.resetModules();
    }
  });

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
              objectId: SCHEMA_OBJECT_ID,
              hash: SCHEMA_HASH,
            }),
            { status: 201 },
          );
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const events: unknown[] = [];
    const { pushSchema } = await import("./catalogue-project.js");
    const result = await pushSchema({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schemaDir: root,
      onEvent: (event) => events.push(event),
    });

    expect(result).toEqual({
      hash: SCHEMA_HASH,
      schemaFile: join(root, "schema.ts"),
      status: "published",
      objectId: SCHEMA_OBJECT_ID,
    });
    expect(publishBody.schema.tables.todos.columns.map((column: any) => column.name)).toEqual([
      "title",
      "ownerId",
    ]);
    expect(events).toEqual([
      { type: "schema-loaded", schemaFile: join(root, "schema.ts") },
      { type: "schema-published", hash: SCHEMA_HASH, objectId: SCHEMA_OBJECT_ID },
    ]);
  });

  it("pushPermissions publishes permissions against an explicit schema hash and uses the current permissions head as expected parent", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), schemaSource());
    await writeFile(join(root, "permissions.ts"), permissionsSource());

    const previousHead = {
      schemaHash: SCHEMA_HASH,
      version: 2,
      parentBundleObjectId: "11111111-1111-1111-1111-111111111111",
      bundleObjectId: "22222222-2222-2222-2222-222222222222",
    };
    const nextHead = {
      schemaHash: SCHEMA_HASH,
      version: 3,
      parentBundleObjectId: previousHead.bundleObjectId,
      bundleObjectId: "33333333-3333-3333-3333-333333333333",
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
          return new Response(JSON.stringify({ head: nextHead }), { status: 201 });
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const { pushPermissions } = await import("./catalogue-project.js");
    const result = await pushPermissions({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schemaDir: root,
      schemaHash: SCHEMA_HASH,
    });

    expect(result).toEqual({
      schemaHash: SCHEMA_HASH,
      permissionsFile: join(root, "permissions.ts"),
      previousHead,
      head: nextHead,
    });
    expect(permissionsBody.schemaHash).toBe(SCHEMA_HASH);
    expect(permissionsBody.expectedParentBundleObjectId).toBe(previousHead.bundleObjectId);
    expect(Object.keys(permissionsBody.permissions)).toContain("todos");
  });

  it("deploy skips permissions publishing when permissions.ts is missing", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), schemaSource());

    let schemaBody: any;
    const fetchCalls: string[] = [];

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init?: RequestInit) => {
        fetchCalls.push(input);
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [] }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/schemas`)) {
          schemaBody = JSON.parse(String(init?.body));
          return new Response(
            JSON.stringify({
              objectId: SCHEMA_OBJECT_ID,
              hash: SCHEMA_HASH,
            }),
            { status: 201 },
          );
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const events: unknown[] = [];
    const { deploy } = await import("./catalogue-project.js");
    const result = await deploy({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schemaDir: root,
      onEvent: (event) => events.push(event),
    });

    expect(result.schema).toEqual({
      hash: SCHEMA_HASH,
      schemaFile: join(root, "schema.ts"),
      status: "published",
      objectId: SCHEMA_OBJECT_ID,
    });
    expect(schemaBody.schema.tables.todos.columns.map((column: any) => column.name)).toEqual([
      "title",
      "ownerId",
    ]);
    expect(fetchCalls).toEqual([
      `${SERVER_URL}/apps/${APP_ID}/schemas`,
      `${SERVER_URL}/apps/${APP_ID}/admin/schemas`,
    ]);
    expect(events).toContainEqual({ type: "schema-loaded", schemaFile: join(root, "schema.ts") });
    expect(events).toContainEqual({
      type: "schema-published",
      hash: SCHEMA_HASH,
      objectId: SCHEMA_OBJECT_ID,
    });
    expect(events).toContainEqual({
      type: "permissions-skipped",
      reason: "missing-permissions-file",
    });
  });

  it("deploy publishes permissions when permissions.ts exists", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), schemaSource());
    await writeFile(join(root, "permissions.ts"), permissionsSource());

    const previousHead = {
      schemaHash: SCHEMA_HASH,
      version: 4,
      parentBundleObjectId: null,
      bundleObjectId: "44444444-4444-4444-4444-444444444444",
    };
    let schemaBody: any;
    let permissionsBody: any;
    const fetchCalls: string[] = [];

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init?: RequestInit) => {
        fetchCalls.push(input);
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [] }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/schemas`)) {
          schemaBody = JSON.parse(String(init?.body));
          return new Response(
            JSON.stringify({
              objectId: SCHEMA_OBJECT_ID,
              hash: SCHEMA_HASH,
            }),
            { status: 201 },
          );
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/permissions/head`)) {
          return new Response(JSON.stringify({ head: previousHead }), { status: 200 });
        }
        if (input.endsWith(`/apps/${APP_ID}/admin/permissions`)) {
          permissionsBody = JSON.parse(String(init?.body));
          return new Response(
            JSON.stringify({
              head: {
                schemaHash: SCHEMA_HASH,
                version: 5,
                parentBundleObjectId: previousHead.bundleObjectId,
                bundleObjectId: "55555555-5555-5555-5555-555555555555",
              },
            }),
            { status: 201 },
          );
        }
        throw new Error(`Unexpected fetch: ${input}`);
      }),
    );

    const { deploy } = await import("./catalogue-project.js");
    const result = await deploy({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schemaDir: root,
    });

    expect(result.schema).toEqual({
      hash: SCHEMA_HASH,
      schemaFile: join(root, "schema.ts"),
      status: "published",
      objectId: SCHEMA_OBJECT_ID,
    });
    expect(schemaBody.schema.tables.todos.columns.map((column: any) => column.name)).toEqual([
      "title",
      "ownerId",
    ]);
    expect(permissionsBody.schemaHash).toBe(SCHEMA_HASH);
    expect(permissionsBody.expectedParentBundleObjectId).toBe(previousHead.bundleObjectId);
    expect(Object.keys(permissionsBody.permissions)).toContain("todos");
    expect(fetchCalls).toEqual([
      `${SERVER_URL}/apps/${APP_ID}/schemas`,
      `${SERVER_URL}/apps/${APP_ID}/admin/schemas`,
      `${SERVER_URL}/apps/${APP_ID}/admin/permissions/head`,
      `${SERVER_URL}/apps/${APP_ID}/admin/permissions`,
    ]);
  });
});
