// `jazzWorker` with no JAZZ_CORE_SECRET: the core makes its own secret and
// hands it to the backend over its binding only.
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

describe.skipIf(!hostBuilt && !process.env.CI)("jazzWorker without a core secret", () => {
  let script: string;
  beforeAll(async () => {
    script = await bundleWorker(join(__dirname, "one-step-worker.ts"));
  });

  async function worker(
    persist?: string,
    secret: string | null = null,
  ): Promise<DurableObjectCore> {
    const started = await startDurableObjectCore({ script, backend: true, persist, secret });
    cleanups.push(() => started.dispose());
    return started;
  }

  it("writes and reads through the app's routes", async () => {
    const { url } = await worker();
    const posted = await fetch(`${url}/todos`, { method: "POST", body: "first" });
    expect(posted.status, await posted.clone().text()).toBe(200);
    expect(await (await fetch(`${url}/todos`)).json()).toEqual(["first"]);
  });

  it("keeps its data, app and secret across a restart", async () => {
    const persist = await mkdtemp(join(tmpdir(), "jazz-do-one-step-"));
    cleanups.push(() => rm(persist, { recursive: true, force: true }));
    const first = await worker(persist);
    expect((await fetch(`${first.url}/todos`, { method: "POST", body: "kept" })).status).toBe(200);
    const appId = await (await fetch(`${first.url}/app-id`)).text();
    const secret = await first.miniflare
      .getDurableObjectNamespace("JAZZ")
      .then((ns) =>
        (
          ns.get(ns.idFromName("jazz")) as unknown as { coreCredential(): Promise<string> }
        ).coreCredential(),
      );
    expect(secret).toMatch(/^[0-9a-f]{64}$/);
    await first.dispose();
    const again = await worker(persist);
    expect(await (await fetch(`${again.url}/todos`)).json()).toEqual(["kept"]);
    expect(await (await fetch(`${again.url}/app-id`)).text()).toBe(appId);
    const ns = await again.miniflare.getDurableObjectNamespace("JAZZ");
    const stub = ns.get(ns.idFromName("jazz")) as unknown as { coreCredential(): Promise<string> };
    expect(await stub.coreCredential()).toBe(secret);
  });

  it("hands out JAZZ_CORE_SECRET when the Worker sets one", async () => {
    const configured = "configured-core-secret-".repeat(2);
    const { miniflare } = await worker(undefined, configured);
    const ns = await miniflare.getDurableObjectNamespace("JAZZ");
    const stub = ns.get(ns.idFromName("jazz")) as unknown as { coreCredential(): Promise<string> };
    expect(await stub.coreCredential()).toBe(configured);
  });
});
