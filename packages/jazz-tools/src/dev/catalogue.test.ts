import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";

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

describe("dev catalogue API exports", () => {
  it("exports catalogue operations from jazz-tools/dev", async () => {
    const dev = await import("./index.js");

    expect(typeof dev.pushSchema).toBe("function");
    expect(typeof dev.pushPermissions).toBe("function");
    expect(typeof dev.pushMigration).toBe("function");
    expect(typeof dev.deploy).toBe("function");
  });

  it("keeps deploy compatible across dev and testing entrypoints", async () => {
    const dev = await import("./index.js");
    const testing = await import("../testing/index.js");

    expect(testing.deploy).toBe(dev.deploy);
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
      'Warning: table "todos" has no explicit insert policy in permissions.ts; enforcing runtimes default to deny.',
    );
    expect(schemaPublishBody.schema.todos.columns.map((column: any) => column.name)).toEqual([
      "title",
      "ownerId",
    ]);
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
        'Warning: table "todos" has no explicit insert policy in permissions.ts; enforcing runtimes default to deny.',
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
          return new Response(JSON.stringify({ schema: storedSchema, publishedAt: 0 }), {
            status: 200,
          });
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
        'Warning: table "todos" has no explicit read policy in permissions.ts; enforcing runtimes default to deny.',
        'Warning: table "todos" has no explicit insert policy in permissions.ts; enforcing runtimes default to deny.',
        'Warning: table "todos" has no explicit update policy in permissions.ts; enforcing runtimes default to deny.',
        'Warning: table "todos" has no explicit delete policy in permissions.ts; enforcing runtimes default to deny.',
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
          return new Response(JSON.stringify({ schema: storedSchema, publishedAt: 0 }), {
            status: 200,
          });
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
          return new Response(JSON.stringify({ schema: fromSchema, publishedAt: 0 }), {
            status: 200,
          });
        }
        if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
          return new Response(JSON.stringify({ schema: toSchema, publishedAt: 0 }), {
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
    expect(publishBody.schema.todos.columns.map((column: any) => column.name)).toEqual([
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
    expect(schemaBody.schema.todos.columns.map((column: any) => column.name)).toEqual([
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
    expect(schemaBody.schema.todos.columns.map((column: any) => column.name)).toEqual([
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
