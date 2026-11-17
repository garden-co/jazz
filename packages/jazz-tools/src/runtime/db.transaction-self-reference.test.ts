import { describe, expect, it } from "vitest";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { testApp, testPermissions } from "../testing/test-app.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz } from "../testing/index.js";

// A nullable self-reference must not hide transaction-committed rows from Global reads.
// The string-typed parent column is the control shape without a relation.
const apps = {
  "nullable self-reference": () =>
    testApp({
      tables: {
        categories: {
          columns: { name: "string", parentId: "uuid?" },
          relations: { parent: { rel: "categories", column: "parentId" } },
        },
      },
    }),
  "optional string parent column": () =>
    testApp({
      tables: { categories: { columns: { name: "string", parentId: "string?" } } },
    }),
};

describe("transaction-committed rows in a hierarchical table", () => {
  for (const [shape, makeApp] of Object.entries(apps)) {
    for (const writer of ["backend", "local-first client"] as const) {
      it(`are returned by Global one-shot reads with a ${shape} (${writer})`, async () => {
        const app = makeApp();
        const permissions = {
          format: 1,
          tables: { categories: { read: "true", insert: "true" } },
        } as const;
        const server = await startTestJazz({
          app,
          permissions,
          coreOptions: { inMemory: true },
        });
        let context: ReturnType<typeof createJazzContext> | undefined;
        let client: Awaited<ReturnType<typeof createDb>> | undefined;
        try {
          let db;
          if (writer === "backend") {
            context = createJazzContext({
              appId: server.appId,
              app,
              permissions: testPermissions(app, permissions),
              serverUrl: server.core.url,
              adminSecret: server.adminSecret,
              backendSecret: server.backendSecret,
              driver: { type: "memory" },
              env: "test",
            });
            db = context.asBackend();
          } else {
            client = await createDb(await localAccountConfig(server.appId, server.url));
            db = client;
          }

          const rows: { id: string; name: string; parentId: string | null }[] = [];
          let parentId: string | null = null;
          for (const name of ["root", "child", "grandchild"]) {
            const write = await db.transaction(async (tx) => {
              const row = tx.insert(app.categories, { name, parentId });
              expect(await tx.one(app.categories.where({ id: row.id }))).toMatchObject({ name });
              return row;
            });
            const row = await write.wait({ tier: "global" });
            rows.push({ id: row.id, name, parentId });
            parentId = row.id;
          }

          for (const row of rows) {
            expect(
              await db.one(app.categories.where({ id: row.id }), { tier: "remote" }),
            ).toMatchObject(row);
          }
          expect(await db.all(app.categories, { tier: "remote" })).toHaveLength(3);
        } finally {
          await client?.shutdown();
          await context?.shutdown();
          await server.stop();
        }
      }, 60_000);
    }
  }
});
