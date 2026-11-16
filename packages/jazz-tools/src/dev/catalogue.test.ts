import { afterEach, describe, expect, it, vi } from "vitest";
import { testApp } from "../testing/index.js";
import { boundApp } from "../typed-app.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import { createNapiNativeRuntimeAdapter } from "../runtime/testing/napi-runtime-test-utils.js";

const APP_ID = "test-app";
const SERVER_URL = "http://localhost:1625";
const ADMIN_SECRET = "admin-secret";

afterEach(() => {
  vi.unstubAllGlobals();
});

const todosApp = testApp({
  tables: { todos: { columns: { title: "string", ownerId: "string" } } },
});
const todosPermissions = {
  format: 1,
  tables: { todos: { read: "ownerId = session.identity.subject" } },
};

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
    const app = testApp({
      tables: {
        users: {
          columns: { name: "string" },
          relations: {
            filesViaOwner: { reverse: "files", relation: "owner" },
            commentsViaAuthor: { reverse: "comments", relation: "author" },
          },
        },
        files: {
          columns: {
            ownerId: "uuid",
            // base64 of [0, 1, 127, 255]
            contents: { type: "bytes", default: "AAF//w==" },
            mediaType: { enum: ["image/png", "text/plain"], default: "text/plain" },
            tags: { array: "string", default: ["draft", "review"] },
          },
          relations: {
            owner: { rel: "users", column: "ownerId" },
            commentsViaFile: { reverse: "comments", relation: "file" },
            commentsViaAttachments: { reverse: "comments", relation: "attachments" },
          },
        },
        comments: {
          columns: {
            fileId: "uuid",
            authorId: { type: "uuid", optional: true, default: null },
            body: "string",
            attachmentIds: { array: "uuid", default: [] },
            status: { enum: ["open", "resolved"], default: "open" },
          },
          relations: {
            file: { rel: "files", column: "fileId" },
            author: { rel: "users", column: "authorId" },
            attachments: { rel: "files", column: "attachmentIds" },
          },
          indexes: ["fileId", "status"],
        },
      },
    });
    await createNapiNativeRuntimeAdapter(app.wasmSchema, {});

    expect(serializeRuntimeSchema(app.wasmSchema)).toContain("__jazzRuntimeSchema");
  });
});

describe("dev catalogue push behavior", () => {
  // Was: deploy({ schemaDir }) from schema.ts/permissions.ts (catalogue-project.ts,
  // removed). The app and its permissions now come from the files; the
  // schemaFile and the permissions-loaded/-published events went with it.
  it.each(["initial", "permissions", "unchanged"])(
    "deploy reports %s publication without separate push requests",
    async (kind) => {
      const { deploy } = await import("./catalogue.js");
      const hash = boundApp(todosApp.wasmSchema)!.schema.current;
      let body: any;
      const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
        if (input.endsWith("/migrations/graph"))
          return new Response(
            JSON.stringify({
              schemas: kind === "initial" ? [] : [hash],
              migrations: [],
              latestBundleSchemaHash: kind === "initial" ? null : hash,
            }),
          );
        expect(input.endsWith("/admin/bundles")).toBe(true);
        body = JSON.parse(String(init?.body));
        return new Response(
          JSON.stringify({
            changed: kind !== "unchanged",
            published: { schemas: kind === "initial" ? [hash] : [], migrations: [] },
          }),
        );
      });
      vi.stubGlobal("fetch", fetchMock);
      const result = await deploy({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        app: todosApp,
        permissions: todosPermissions,
      });
      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(body.targetSchemaHash).toBe(hash);
      expect(body.schemas).toHaveLength(kind === "initial" ? 1 : 0);
      expect(Object.keys(body.permissions)).toContain("todos");
      expect(result.changed).toBe(kind !== "unchanged");
      expect(result.schema).toEqual({
        hash,
        status: kind === "initial" ? "published" : "already-stored",
      });
    },
  );

  it("deploy warns for each table jazz.permissions.json gives no policy, not for explicit false", async () => {
    const { deploy } = await import("./catalogue.js");
    const app = testApp({
      tables: {
        notes: { columns: { body: "string" } },
        drafts: { columns: { body: "string" } },
        audit: { columns: { body: "string" } },
        todos: { columns: { title: "string" } },
      },
    });
    const hash = boundApp(app.wasmSchema)!.schema.current;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string) =>
        input.endsWith("/migrations/graph")
          ? new Response(
              JSON.stringify({ schemas: [], migrations: [], latestBundleSchemaHash: null }),
            )
          : new Response(
              JSON.stringify({ changed: true, published: { schemas: [hash], migrations: [] } }),
            ),
      ),
    );
    const result = await deploy({
      appId: APP_ID,
      serverUrl: SERVER_URL,
      adminSecret: ADMIN_SECRET,
      app,
      permissions: {
        format: 1,
        tables: {
          // Listed with nothing allowed: still no policy.
          drafts: {},
          // Only the backend uses it, and the file says so.
          audit: { read: "false", insert: "false", update: "false", delete: "false" },
          todos: { read: "true", insert: "false", update: "false", delete: "false" },
        },
      },
    });
    const warning = (table: string) =>
      `table "${table}" has no policies in jazz.permissions.json, so users can't read, insert, update or delete its rows. ` +
      "Your backend isn't checked when it acts as the system, so if only it uses this table, that's fine: " +
      `write \`"${table}": { "read": "false" }\` to say so and silence this warning. ` +
      "Don't add allow rules just to silence it.";
    expect(result.warnings).toEqual([warning("drafts"), warning("notes")]);
  });

  it("deploy rejects policies compiled against an outdated schema before contacting the server", async () => {
    const { deploy } = await import("./catalogue.js");
    // Permissions written for a column the schema no longer has.
    const permissions = {
      format: 1,
      tables: { todos: { read: "owner = session.identity.subject" } },
    };
    const app = testApp({ tables: { todos: { columns: { title: "string" } } } });
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      deploy({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        app,
        permissions,
      }),
    ).rejects.toThrow("owner");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("deploy rejects omitted permissions before contacting the server", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { deploy } = await import("./catalogue.js");
    await expect(
      // @ts-expect-error Exercise JavaScript callers that omit the required bundle.
      deploy({
        appId: APP_ID,
        serverUrl: SERVER_URL,
        adminSecret: ADMIN_SECRET,
        app: testApp({ tables: { todos: { columns: { title: "string" } } } }),
      }),
    ).rejects.toThrow(/deploy requires the app's jazz.permissions.json/);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
