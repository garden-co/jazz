import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { defineMigration, exportLocalFirstSecret, schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "./index.js";

const before = { entries: s.table({ text: s.string() }, {}) };
const after = { ...before, controls: s.table({ value: s.string() }, {}) };
const oldApp = s.defineApp(before);
const newApp = s.defineApp(after);
const oldPermissions = s.definePermissions(oldApp, ({ policy }) => {
  policy.entries.allowRead.always();
});
const newPermissions = s.definePermissions(newApp, ({ policy }) => {
  policy.entries.allowRead.always();
  policy.controls.allowRead.always();
});

describe("offline persistent schema bootstrap", () => {
  for (const reader of [false, true]) {
    for (const restart of [false, true]) {
      it(`${reader ? "read-only local-first" : "backend"} recovers published lineage with server ${restart ? "restarted" : "running"}`, async () => {
        const directory = await mkdtemp(join(tmpdir(), "jazz-offline-schema-test-"));
        const settings = {
          appId: randomUUID(),
          dataDir: join(directory, "server"),
          adminSecret: randomUUID(),
          backendSecret: randomUUID(),
          allowLocalFirstAuth: true,
        };
        let server = await startLocalJazzServer({
          ...settings,
          schema: oldApp,
          permissions: oldPermissions,
        });
        type Session = Awaited<ReturnType<typeof createJazzSession>>;
        const sessions = new Set<Session>();
        let readerSecret: string | undefined;
        async function open(
          clientApp: Parameters<typeof createJazzSession>[0]["app"],
          name: string,
          asReader = reader,
        ) {
          const session = await createJazzSession({
            appId: settings.appId,
            serverUrl: server.url,
            app: clientApp,
            permissions: clientApp === newApp ? newPermissions : oldPermissions,
            driver: { type: "persistent", dataPath: join(directory, name) },
            initial: asReader ? "local-first" : { backendSecret: settings.backendSecret },
          });
          sessions.add(session);
          if (asReader) {
            if (readerSecret) await session.restoreLocalFirst(readerSecret);
            else readerSecret = exportLocalFirstSecret(session.getSnapshot().account!);
          }
          const snapshot = session.getSnapshot();
          if (!snapshot.client) throw snapshot.error ?? new Error(snapshot.status);
          return { session, db: snapshot.client.db };
        }
        async function close(session: Session) {
          await session.getSnapshot().client?.shutdown({ waitForSync: false });
          await session.close();
          sessions.delete(session);
        }
        try {
          await deploy({
            ...settings,
            serverUrl: server.url,
            schema: oldApp,
            permissions: oldPermissions,
          });
          const seed = await open(oldApp, "seed", false);
          await seed.db
            .upsert(oldApp.entries, randomUUID(), { text: "retained row" })
            .wait({ tier: "global" });
          await close(seed.session);
          const original = await open(oldApp, "reader");
          expect(await original.db.all(oldApp.entries, { tier: "global" })).toHaveLength(1);
          await close(original.session);
          await deploy({
            ...settings,
            serverUrl: server.url,
            schema: newApp,
            permissions: newPermissions,
            migration: defineMigration({
              from: before,
              to: after,
              createTables: { controls: true },
            }),
          });
          if (restart) {
            const port = server.port;
            await server.stop();
            server = await startLocalJazzServer({ ...settings, port });
          }
          const upgraded = await open(newApp, "reader");
          expect(await upgraded.db.all(newApp.entries, { tier: "global" })).toMatchObject([
            { text: "retained row" },
          ]);
          expect(await upgraded.db.all(newApp.controls, { tier: "global" })).toEqual([]);
          await close(upgraded.session);
          const reopened = await open(newApp, "reader");
          expect(await reopened.db.all(newApp.entries, { tier: "local" })).toHaveLength(1);
          await close(reopened.session);
          // An incompatible, unpublished target cannot borrow B's admission.
          const incompatible = s.defineApp({ entries: s.table({ text: s.boolean() }, {}) });
          const rejected = await open(incompatible, "reader");
          await expect(rejected.db.all(incompatible.entries, { tier: "global" })).rejects.toThrow(
            /awaiting published catalogue admission/,
          );
          await close(rejected.session);
          const retained = await open(newApp, "reader");
          expect(await retained.db.all(newApp.entries, { tier: "local" })).toHaveLength(1);
          await close(retained.session);
        } finally {
          for (const session of sessions) await close(session);
          await server.stop();
          await rm(directory, { recursive: true, force: true });
        }
      }, 60_000);
    }
  }
});
