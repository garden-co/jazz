import { describe, expect, it } from "vitest";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

// A nullable self-reference must not hide transaction-committed rows from Global reads.
// The string-typed parent column is the control shape without a relation.
const apps = {
  "nullable self-reference": () =>
    s.defineApp({
      categories: s.table(
        { name: s.string(), parentId: s.uuid().optional() },
        { parent: s.rel("categories", "parentId") },
      ),
    }),
  "optional string parent column": () =>
    s.defineApp({
      categories: s.table({ name: s.string(), parentId: s.string().optional() }, {}),
    }),
};

describe("transaction-committed rows in a hierarchical table", () => {
  for (const [shape, makeApp] of Object.entries(apps)) {
    for (const writer of ["backend", "local-first client"] as const) {
      it(`are returned by Global one-shot reads with a ${shape} (${writer})`, async () => {
        const app = makeApp() as ReturnType<(typeof apps)["nullable self-reference"]>;
        const permissions = definePermissions(app, ({ policy }) => {
          policy.categories.allowRead.always();
          policy.categories.allowInsert.always();
        });
        const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
        let context: ReturnType<typeof createJazzContext> | undefined;
        let client: Awaited<ReturnType<typeof createDb>> | undefined;
        try {
          await deploy({
            serverUrl: server.url,
            appId: server.appId,
            adminSecret: server.adminSecret,
            schema: app,
            permissions,
          });
          let db;
          if (writer === "backend") {
            context = createJazzContext({
              appId: server.appId,
              app,
              permissions,
              serverUrl: server.url,
              adminSecret: server.adminSecret,
              backendSecret: server.backendSecret,
              driver: { type: "memory" },
              tier: "local",
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
              await db.one(app.categories.where({ id: row.id }), { tier: "global" }),
            ).toMatchObject(row);
          }
          expect(await db.all(app.categories, { tier: "global" })).toHaveLength(3);
        } finally {
          await client?.shutdown();
          await context?.shutdown();
          await server.stop();
        }
      }, 60_000);
    }
  }
});
