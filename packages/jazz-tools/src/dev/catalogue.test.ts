import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

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

describe("dev catalogue pending operations", () => {
  it("pushMigration rejects because it is not implemented yet", async () => {
    const { pushMigration } = await import("./index.js");

    await expect(
      pushMigration({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        migrationsDir: "/unused",
        fromHash: "from-hash",
        toHash: "to-hash",
      }),
    ).rejects.toThrow("pushMigration is not implemented yet.");
  });

  it("deploy rejects because it is not implemented yet", async () => {
    const { deploy } = await import("./index.js");

    await expect(
      deploy({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        schemaDir: "/unused",
        migrationsDir: "/unused",
      }),
    ).rejects.toThrow("deploy is not implemented yet.");
  });
});

describe("dev catalogue push behavior", () => {
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
    const { pushSchema } = await import("./index.js");
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

    const { pushPermissions } = await import("./index.js");
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

  it("pushSchemaCatalogue still publishes permissions when permissions.ts exists", async () => {
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

    const { pushSchemaCatalogue } = await import("./index.js");
    const result = await pushSchemaCatalogue({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      schemaDir: root,
    });

    expect(result).toEqual({ hash: SCHEMA_HASH });
    expect(schemaBody.schema.todos.columns.map((column: any) => column.name)).toEqual([
      "title",
      "ownerId",
    ]);
    expect(permissionsBody.schemaHash).toBe(SCHEMA_HASH);
    expect(permissionsBody.expectedParentBundleObjectId).toBe(previousHead.bundleObjectId);
    expect(Object.keys(permissionsBody.permissions)).toContain("todos");
    expect(fetchCalls).toEqual([
      `${SERVER_URL}/apps/${APP_ID}/admin/schemas`,
      `${SERVER_URL}/apps/${APP_ID}/admin/permissions/head`,
      `${SERVER_URL}/apps/${APP_ID}/admin/permissions`,
    ]);
  });
});
