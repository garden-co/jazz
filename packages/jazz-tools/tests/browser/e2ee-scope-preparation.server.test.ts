import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createAccountDbWithRuntimeSource } from "../../src/accounts/context.js";
import { DefaultRuntimeSource } from "../../src/runtime/default-runtime-source.js";
import type { RuntimeClientContext } from "../../src/runtime/runtime-source.js";
import type { JazzClient } from "../../src/runtime/client.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { toWriteRecord } from "../../src/runtime/value-converter.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

class PreparationSource extends DefaultRuntimeSource {
  client!: JazzClient;
  override createClient(context: RuntimeClientContext): JazzClient {
    this.client = super.createClient(context);
    return this.client;
  }
}

it.each([false, true])(
  "resolves a cold scope during atomic preparation (reject: %s)",
  async (reject) => {
    const server = await getJazzServerInfo(`e2ee-scope-preparation-${crypto.randomUUID()}`);
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      notes: s.table(
        { projectId: s.uuid(), scope: s.string() },
        { project: s.rel("projects", "projectId") },
      ),
    });
    const permissions = definePermissions(app, ({ policy }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
    });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
      await deploy({ ...server, schema: app, permissions });
      const account = await acquireBrowserTestAccount(server);
      const config = {
        appId: server.appId,
        serverUrl: server.serverUrl,
        account,
        driver: { type: "memory" as const },
      };
      const source = new PreparationSource();
      const db = await createAccountDbWithRuntimeSource(config, source);
      clients.push(db);
      const observer = await createDb(config);
      clients.push(observer);
      // Attach the schema without requesting authority coverage. Public reads
      // propagate even at the local tier; a local catalogue probe does not.
      await db.tableIdentity(app.projects, true);
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Atomic scope" });
      const transactionId = tx.openTransactionId();
      source.client.prepareTransaction(transactionId, async () => {
        const scope = await db.tableIdentity(app.projects);
        expect(scope).not.toBeNull();
        if (reject) throw new Error("Scope preparation rejected");
        source.client
          .getRuntime()
          .insert(
            app.notes._table,
            toWriteRecord({ projectId: project.id, scope }, app.notes._schema, app.notes._table),
            JSON.stringify({ transaction_id: transactionId }),
          );
      });
      const handle = tx.commit();
      expect(handle).not.toBeInstanceOf(Promise);
      if (reject) {
        await expect(handle.wait({ tier: "global" })).rejects.toThrow("Scope preparation rejected");
        await tx.rollback();
        expect(await observer.all(app.projects, { tier: "edge" })).toEqual([]);
        expect(await observer.all(app.notes, { tier: "edge" })).toEqual([]);
        return;
      }
      await handle.wait({ tier: "global" });
      expect(await observer.all(app.projects, { tier: "edge" })).toMatchObject([
        { id: project.id, title: "Atomic scope" },
      ]);
      expect(await observer.all(app.notes, { tier: "edge" })).toMatchObject([
        { projectId: project.id, scope: await observer.tableIdentity(app.projects) },
      ]);
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await stopJazzServer(server.serverUrl);
    }
  },
  30_000,
);
