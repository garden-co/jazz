/// <reference types="vite/client" />
import { describe, expect, it } from "vitest";
import {
  createBrowserTestDb as createDb,
  uniqueDbName,
  withTimeout,
  waitForCondition,
} from "./support.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { setBrowserFollowerProbeTimingForTest } from "../../src/runtime/native-runtime/browser-follower-connection.js";
import {
  app,
  todos,
  allTodos,
  workerFaultBundleUrl,
  LIVENESS_TEST_PROBE_TIMING,
  LIVENESS_TEST_SIGNAL_MS,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

describe("automatic SharedWorker mutation recovery", () => {
  const { track, untrack } = useSharedWorkerBridgeHarness();

  it.each(["before delivery", "after commit with acknowledgement lost"])(
    "persists the original offline insert and delete after worker death %s",
    async (phase) => {
      setBrowserFollowerProbeTimingForTest(LIVENESS_TEST_PROBE_TIMING);
      const capability = uniqueDbName("worker-recovery");
      const workerUrl = new URL(await workerFaultBundleUrl(), location.href);
      workerUrl.searchParams.set("followerFault", capability);
      const control = new BroadcastChannel(capability);
      const command = (type: string, reply: string) =>
        withTimeout(
          new Promise<void>((resolve) => {
            const listener = (event: MessageEvent<{ type: string }>) => {
              if (event.data.type !== reply) return;
              control.removeEventListener("message", listener);
              resolve();
            };
            control.addEventListener("message", listener);
            control.postMessage({ type });
          }),
          5_000,
          `Worker did not acknowledge ${type}`,
        );
      const config = {
        appId: uniqueDbName("worker-recovery-app"),
        secret: generateAuthSecret(),
        driver: { type: "persistent" as const, dbName: uniqueDbName("worker-recovery-root") },
        schema: app,
        runtimeSources: { brokerWorkerUrl: workerUrl.href, wasmVersion: "worker-recovery-test" },
      };
      const db = track(await createDb(config));
      let verified = false;
      try {
        const anchor = db.insert(todos, { title: "persisted before death", done: false });
        await anchor.wait({ tier: "local" });
        await db.all(allTodos, { tier: "local" });
        if (phase === "before delivery") {
          await command("hold-frames", "holding-frames");
          await command("die", "dying");
        } else await command("drop-outbound-frames", "dropping-outbound-frames");

        const created = db.insert(todos, { title: "created during worker failure", done: false });
        const deleted = db.delete(todos, anchor.value.id);
        let settled = false;
        const durability = Promise.all([
          created.wait({ tier: "local" }),
          deleted.wait({ tier: "local" }),
        ]).then(() => {
          settled = true;
        });

        if (phase !== "before delivery") {
          // A separate foreground reads the durable worker through an unaffected
          // port, proving both changes exist before killing the owner. Only the
          // original writer's frames (including receipts) have been dropped.
          const observer = await createDb(config);
          try {
            await waitForCondition(
              async () => {
                const rows = await observer.all(allTodos, { tier: "local" });
                return rows.length === 1 && rows[0]!.id === created.value.id;
              },
              10_000,
              "Worker did not commit the insert and delete before its death",
            );
            expect(settled).toBe(false);
          } finally {
            await withTimeout(observer.shutdown(), 5_000, "Observer shutdown did not finish");
          }
          await command("die", "dying");
        }

        await withTimeout(
          durability,
          LIVENESS_TEST_SIGNAL_MS,
          "Original local durability handles did not recover",
        );
        const rows = await db.all(allTodos, { tier: "local" });
        expect(rows.map((row) => row.id)).toEqual([created.value.id]);
        await withTimeout(
          db.shutdown(),
          10_000,
          "Recovered Db shutdown retained a dead lease port",
        );
        untrack(db);
        const reopened = track(await createDb(config));
        expect((await reopened.all(allTodos, { tier: "local" })).map((row) => row.id)).toEqual([
          created.value.id,
        ]);
        verified = true;
      } finally {
        // Preserve the bounded assertion as the regression signal; deliberately
        // broken fixtures must not enter the harness's ordinary cleanup loop.
        untrack(db);
        void db.shutdown().catch(() => undefined);
        if (!verified) control.postMessage({ type: "die" });
        control.close();
      }
    },
    45_000,
  );
});
