// The all-Cloudflare deployment: an app's createJazz backend in a Durable
// Object of the same Worker as its core, on jazz-wasm (no jazz-napi), in
// local workerd. Writes and reads through the app's own routes, and its
// data across a restart.
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { bundleWorker } from "./bundle.js";
import { hostBuilt } from "./host.js";
import { startDurableObjectCore, type DurableObjectCore } from "./harness.js";

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

describe.skipIf(!hostBuilt && !process.env.CI)("createJazz in a Cloudflare Worker", () => {
  let script: string;
  beforeAll(async () => {
    script = await bundleWorker(join(__dirname, "backend-worker.ts"));
  });

  async function worker(persist?: string): Promise<DurableObjectCore> {
    const started = await startDurableObjectCore({ script, backend: true, persist });
    cleanups.push(() => started.dispose());
    return started;
  }

  it("writes and reads through the app's routes on jazz.db", async () => {
    const { url } = await worker();
    const posted = await fetch(`${url}/todos`, { method: "POST", body: "first" });
    expect(posted.status, await posted.clone().text()).toBe(200);
    expect(await (await fetch(`${url}/todos`)).json()).toEqual(["first"]);
  });

  it("keeps the app's data across a restart", async () => {
    const persist = await mkdtemp(join(tmpdir(), "jazz-do-backend-"));
    cleanups.push(() => rm(persist, { recursive: true, force: true }));
    const first = await worker(persist);
    expect((await fetch(`${first.url}/todos`, { method: "POST", body: "kept" })).status).toBe(200);
    await first.dispose();
    const again = await worker(persist);
    expect(await (await fetch(`${again.url}/todos`)).json()).toEqual(["kept"]);
  });
});
