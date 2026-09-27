import { describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { schema as s, generateAuthSecret } from "../../src/index.js";
import { createBrowserTestDb as createDb } from "./account-fixtures.js";
import type { Db } from "../../src/runtime/db.js";

declare const __JAZZ_ABSTRACT_BENCH__: string;

const app = s.defineApp({
  folders: s.table({ name: s.string(), description: s.string() }, {}),
  tasks: s.table(
    { title: s.string(), body: s.string(), folderId: s.uuid() },
    { folder: s.rel("folders", "folderId") },
  ),
});

// Opt-in performance qualification, not a wall-clock correctness gate.
// Persistent uses foreground plus durable worker; memory is a single runtime.
// Local-durable pending writes, no Core; the storage lanes differ in topology.
// Reopen resets database/page caches, while browser/WASM/OS caches remain warm.
// Release WASM: pnpm --filter jazz-wasm build, then
// node dev/artifacts/stage-native-fingerprints.mjs --workspace.
// JAZZ_ABSTRACT_BENCH=1 pnpm exec vitest run --maxWorkers=1
// --config vitest.config.browser.ts tests/browser/witness-tradeoffs.abstract-bench.test.ts
// The report contains no account secrets, installation IDs or real customer data.
describe.skipIf(__JAZZ_ABSTRACT_BENCH__ !== "1")("native witness browser tradeoffs", () => {
  for (const count of [150, 1500]) {
    for (const storage of ["memory", "persistent"] as const) {
      it(`${storage}: ${count} rows sharing 15 relation targets`, async () => {
        const config = {
          appId: crypto.randomUUID(),
          secret: generateAuthSecret(),
          driver:
            storage === "memory"
              ? { type: "memory" as const }
              : { type: "persistent" as const, dbName: `witness-tradeoffs-${crypto.randomUUID()}` },
          logLevel: "warn" as const,
        };
        const phases: Record<string, number[]> = {};
        const measure = async <T>(phase: string, operation: () => Promise<T>): Promise<T> => {
          const start = performance.now();
          try {
            return await operation();
          } finally {
            (phases[phase] ??= []).push(performance.now() - start);
          }
        };
        let db: Db | undefined;
        const folderIds: string[] = [];
        const query = app.tasks.include({ folder: true });
        type FixtureRow = {
          id: string;
          title: string;
          body: string;
          folderId: string;
          folder?: { id: string; name: string; description: string };
        };
        const check = (rows: FixtureRow[], included: boolean) => {
          expect(rows).toHaveLength(count);
          const seen = new Set<string>();
          for (const row of rows) {
            const index = Number(row.title.slice("Task ".length));
            expect(index).toBeGreaterThanOrEqual(0);
            expect(index).toBeLessThan(count);
            expect(row.body).toBe(`Body ${index} ${"x".repeat(128)}`);
            expect(row.folderId).toBe(folderIds[index % 15]);
            expect(seen.has(row.id)).toBe(false);
            seen.add(row.id);
            if (included) {
              expect(row.folder).toEqual({
                id: folderIds[index % 15],
                name: `Folder ${index % 15}`,
                description: `Description ${index % 15} ${"y".repeat(2048)}`,
              });
            }
          }
        };
        const read = async (phase: string, included: boolean) => {
          const rows = await measure(phase, () =>
            included ? db!.all(query, { tier: "local" }) : db!.all(app.tasks, { tier: "local" }),
          );
          check(rows, included);
        };
        const subscription = async () => {
          let stop = () => {};
          try {
            const rows = await new Promise<FixtureRow[]>((resolve, reject) => {
              stop = db!.subscribe(
                query,
                {
                  onUpdate: (rows) => {
                    if (rows.length === count) resolve(rows);
                  },
                  onError: reject,
                },
                { tier: "local" },
              );
            });
            return rows;
          } finally {
            stop();
          }
        };
        try {
          db = await measure("initial_open", () => createDb(config));
          await measure("initial_empty_query", () => db!.all(query, { tier: "local" }));
          await measure("seed_local_durable", async () => {
            const result = await db!.transaction((tx) => {
              for (let i = 0; i < 15; i++) {
                const folder = tx.insert(app.folders, {
                  name: `Folder ${i}`,
                  description: `Description ${i} ${"y".repeat(2048)}`,
                });
                folderIds.push(folder.id);
              }
              for (let i = 0; i < count; i++)
                tx.insert(app.tasks, {
                  title: `Task ${i}`,
                  body: `Body ${i} ${"x".repeat(128)}`,
                  folderId: folderIds[i % 15],
                });
            });
            await result.wait({ tier: "local" });
          });
          await read("seeded_flat", false);
          await read("seeded_include", true);
          for (let repeat = 0; repeat < 5; repeat++) {
            await read("warm_flat", false);
            await read("warm_include", true);
          }
          check(await measure("warm_subscription", subscription), true);
          if (storage === "persistent") {
            for (const first of ["flat", "include", "subscription"] as const) {
              await measure("shutdown", () => db!.shutdown());
              db = await measure(`${first}_reopen_runtime`, () => createDb(config));
              if (first === "subscription")
                check(await measure("cold_subscription", subscription), true);
              else await read(`cold_${first}`, first === "include");
              for (let repeat = 0; repeat < 5; repeat++)
                await read(`reopened_${first}_warm_include`, true);
            }
          }
          await commands.writeRealisticBrowserReport(`witness-tradeoffs-${storage}-${count}`, {
            fixture: "native-witness-browser-v1",
            topology: storage === "persistent" ? "foreground-and-worker" : "single-runtime",
            storage,
            count,
            relationTargets: 15,
            relationTextBytes: 2048,
            settlement: "local-pending",
            phases,
          });
          console.info("[witness-tradeoffs]", JSON.stringify({ storage, count, phases }));
        } finally {
          await db?.shutdown();
        }
      }, 600_000);
    }
  }
});
