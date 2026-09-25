import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import { createNapiNativeRuntimeAdapter } from "../runtime/testing/napi-runtime-test-utils.js";

const tempRoots: string[] = [];
const APP_ID = "test-app";
const SERVER_URL = "http://localhost:1625";
const ADMIN_SECRET = "admin-secret";

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
  }, {  }),
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
  policy.todos.allowRead.where({ ownerId: session.user.identity.subject }),
]);
`;
}

describe("dev catalogue API exports", () => {
  it("exports catalogue operations from jazz-tools/dev", async () => {
    const dev = await import("./index.js");

    expect(dev).not.toHaveProperty("pushSchema");
    expect(dev).not.toHaveProperty("pushPermissions");
    expect(dev).not.toHaveProperty("pushMigration");
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
      users: s.table(
        {
          name: s.string(),
        },
        {
          filesViaOwner: s.reverse("files", "owner"),
          commentsViaAuthor: s.reverse("comments", "author"),
        },
      ),
      files: s.table(
        {
          ownerId: s.uuid(),
          contents: s.bytes().default(new Uint8Array([0, 1, 127, 255])),
          mediaType: s.enum("image/png", "text/plain").default("text/plain"),
          tags: s.array(s.string()).default(["draft", "review"]),
        },
        {
          owner: s.rel("users", "ownerId"),
          commentsViaFile: s.reverse("comments", "file"),
          commentsViaAttachments: s.reverse("comments", "attachments"),
        },
      ),
      comments: s
        .table(
          {
            fileId: s.uuid(),
            authorId: s.uuid().optional().default(null),
            body: s.string(),
            attachmentIds: s.array(s.uuid()).default([]),
            status: s.enum("open", "resolved").default("open"),
          },
          {
            file: s.rel("files", "fileId"),
            author: s.rel("users", "authorId"),
            attachments: s.rel("files", "attachmentIds"),
          },
        )
        .indexOnly(["fileId", "status"]),
    };
    const app = s.defineApp(schema);
    await createNapiNativeRuntimeAdapter(app.wasmSchema, {});

    expect(serializeRuntimeSchema(app.wasmSchema)).toContain("__jazzRuntimeSchema");
  });
});

describe("dev catalogue push behavior", () => {
  it.each(["initial", "permissions", "unchanged"])(
    "deploy reports %s publication without separate push requests",
    async (kind) => {
      const { root } = await createWorkspace();
      await writeFile(join(root, "schema.ts"), schemaSource());
      await writeFile(join(root, "permissions.ts"), permissionsSource());
      const { loadCompiledSchema } = await import("../schema-loader.js");
      const { computeSchemaHash } = await import("./catalogue.js");
      const hash = await computeSchemaHash((await loadCompiledSchema(root)).wasmSchema);
      const events: any[] = [];
      let body: any;
      const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
        if (input.endsWith("/migrations/graph"))
          return new Response(
            JSON.stringify({
              schemas: kind === "initial" ? [] : [hash],
              migrations: [],
              activeSchemaHash: kind === "initial" ? null : hash,
            }),
          );
        expect(input.endsWith("/admin/deploy")).toBe(true);
        body = JSON.parse(String(init?.body));
        return new Response(
          JSON.stringify({
            changed: kind !== "unchanged",
            published: { schemas: kind === "initial" ? [hash] : [], migrations: [] },
          }),
        );
      });
      vi.stubGlobal("fetch", fetchMock);
      const { deploy } = await import("./catalogue-project.js");
      const result = await deploy({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        schemaDir: root,
        onEvent: (event) => events.push(event),
      });
      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(body.targetSchemaHash).toBe(hash);
      expect(body.schemas).toHaveLength(kind === "initial" ? 1 : 0);
      expect(Object.keys(body.permissions)).toContain("todos");
      expect(result.changed).toBe(kind !== "unchanged");
      expect(result.schema).toEqual({
        hash,
        schemaFile: join(root, "schema.ts"),
        status: kind === "initial" ? "published" : "already-stored",
      });
      expect(result.warnings.some((warning) => warning.includes("no explicit insert policy"))).toBe(
        true,
      );
      expect(events).toContainEqual({
        type: "permissions-loaded",
        permissionsFile: join(root, "permissions.ts"),
      });
      expect(events.some((event) => event.type === "permissions-published")).toBe(
        kind !== "unchanged",
      );
    },
  );

  it("deploy rejects policies compiled against an outdated schema before contacting the server", async () => {
    const { deploy } = await import("./catalogue.js");
    const oldApp = s.defineApp({ todos: s.table({ owner: s.string() }, {}) });
    const permissions = s.definePermissions(oldApp, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user.identity.subject });
    });
    const app = s.defineApp({ todos: s.table({ title: s.string() }, {}) });
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      deploy({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        schema: app,
        permissions,
      }),
    ).rejects.toThrow("owner");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("deploy rejects missing permissions before contacting the server", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), schemaSource());
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { deploy } = await import("./catalogue-project.js");
    await expect(
      deploy({ appId: APP_ID, serverUrl: SERVER_URL, adminSecret: ADMIN_SECRET, schemaDir: root }),
    ).rejects.toThrow("Create a permissions.ts file");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("deploy rejects omitted in-code permissions before contacting the server", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { deploy } = await import("./catalogue.js");
    await expect(
      // @ts-expect-error Exercise JavaScript callers that omit the required bundle.
      deploy({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        schema: s.defineApp({ todos: s.table({ title: s.string() }, {}) }),
      }),
    ).rejects.toThrow("explicit permissions bundle");
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
