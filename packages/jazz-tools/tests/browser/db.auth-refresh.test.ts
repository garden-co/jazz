import { afterEach, describe, expect, it } from "vitest";
import {
  createDb,
  fetchSchemaHashes,
  publishStoredPermissions,
  type Db,
  type QueryBuilder,
  type TableProxy,
} from "../../src/runtime/index.js";
import { publishStoredSchema } from "../../src/runtime/schema-fetch.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { TestCleanup, uniqueDbName, waitForCondition, waitForQuery } from "./support.js";
import { getTestingServerInfo, getTestingServerJwtForUser } from "./testing-server.js";

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

type Todo = {
  id: string;
  title: string;
  done: boolean;
};

type TodoInit = {
  title: string;
  done: boolean;
};

const todos: TableProxy<Todo, TodoInit> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as TodoInit,
};

const allTodos: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

describe("Db auth refresh browser integration", () => {
  const ctx = new TestCleanup();

  afterEach(async () => {
    await ctx.cleanup();
  });

  it("recovers from auth loss after updateAuthToken and flushes queued local writes", async () => {
    const { appId, serverUrl, adminSecret } = await getTestingServerInfo();

    const dbNameA = uniqueDbName("auth-refresh-a");
    const dbNameB = uniqueDbName("auth-refresh-b");
    const invalidJwt = makeFakeJwt({
      sub: "alice",
      claims: { role: "member" },
      exp: Math.floor(Date.now() / 1000) + 3600,
    });
    const validJwt = await getTestingServerJwtForUser("alice", { role: "member" });

    const writer = ctx.track(
      await createDb({
        appId,
        serverUrl,
        jwtToken: invalidJwt,
        driver: { type: "persistent", dbName: dbNameA },
      }),
    );
    const reader = ctx.track(
      await createDb({
        appId,
        serverUrl,
        jwtToken: validJwt,
        driver: { type: "persistent", dbName: dbNameB },
      }),
    );

    const { hash: publishedSchemaHash } = await publishStoredSchema(serverUrl, {
      adminSecret,
      schema,
    });

    let latestSchemaHash: string | null = publishedSchemaHash;
    await waitForCondition(
      async () => {
        const { hashes } = await fetchSchemaHashes(serverUrl, { adminSecret });
        latestSchemaHash = hashes.at(-1) ?? publishedSchemaHash;
        return latestSchemaHash !== null;
      },
      20_000,
      "expected at least one published schema hash before publishing test permissions",
    );

    await publishStoredPermissions(serverUrl, {
      adminSecret,
      schemaHash: latestSchemaHash!,
      permissions: {
        todos: {
          select: { using: { type: "True" } },
          insert: { with_check: { type: "True" } },
          update: {
            using: { type: "True" },
            with_check: { type: "True" },
          },
          delete: { using: { type: "True" } },
        },
      },
    });

    const marker = `queued-after-auth-loss-${Date.now()}`;
    writer.insert(todos, {
      title: marker,
      done: false,
    });

    await waitForCondition(
      async () => {
        const authState = writer.getAuthState();
        return authState.status === "unauthenticated" && authState.reason === "invalid";
      },
      20_000,
      "writer should transition to unauthenticated after invalid JWT auth failure",
    );

    expect(writer.getAuthState()).toMatchObject({
      status: "unauthenticated",
      reason: "invalid",
      session: {
        user_id: "alice",
      },
    });

    writer.updateAuthToken(validJwt);

    await waitForCondition(
      async () => writer.getAuthState().status === "authenticated",
      20_000,
      "writer should return to authenticated after updateAuthToken",
    );

    await waitForQuery(
      reader,
      allTodos,
      (rows) => rows.some((row) => row.title === marker),
      "queued write should flush after auth refresh",
      20_000,
      "worker",
    );
  });
});

function toBase64Url(value: unknown): string {
  return btoa(JSON.stringify(value)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeFakeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "HS256", typ: "JWT" })}.${toBase64Url(payload)}.bad-signature`;
}
