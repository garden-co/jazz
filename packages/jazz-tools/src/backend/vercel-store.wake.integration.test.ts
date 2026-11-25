import { describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import {
  connect as connectTcp,
  createServer as createNetServer,
  type AddressInfo,
  type Socket,
} from "node:net";
import { createJazz, vercel } from "./index.js";
import {
  app,
  cleanups,
  fakeVercel,
  legacyNamespace,
  onVercel,
  permissions,
  runCleanupsAfterEach,
  sleep,
  startTrustedCore,
} from "./fixtures/vercel-store.js";

runCleanupsAfterEach();

describe("vercel() store: connections waiting for a wake", () => {
  it("never stops the session it just woke for connections that were only waiting for the wake (#6028)", async () => {
    // Vercel's router in front of the core. While the Sandbox is stopped it
    // answers nothing. For a while after each start, the core answers
    // `/health` but holds WebSocket upgrades (a busy core, slow to say hello).
    const core = await startTrustedCore(randomUUID());
    const target = new URL(core.url);
    let fake!: ReturnType<typeof fakeVercel>;
    let busyUntil = 0;
    const passing = new Set<Socket>();
    const router = createNetServer((client) => {
      client.on("error", () => {});
      client.once("data", (head) => {
        const upgrade = /^upgrade:\s*websocket/im.test(head.toString("latin1"));
        if (fake.state.status !== "running" || (upgrade && Date.now() < busyUntil)) {
          // Held: never answered, never closed.
          cleanups.push(async () => client.destroy());
          return;
        }
        const upstream = connectTcp(Number(target.port), target.hostname, () => {
          upstream.write(head);
          client.pipe(upstream).pipe(client);
        });
        passing.add(client);
        const end = () => {
          passing.delete(client);
          client.destroy();
          upstream.destroy();
        };
        client.on("close", end);
        upstream.on("error", end).on("close", end);
      });
    });
    await new Promise<void>((resolve) => router.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => router.close(resolve)));
    const url = `http://127.0.0.1:${(router.address() as AddressInfo).port}`;
    fake = fakeVercel({ appId: core.appId, url });
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });
    const jazz = await createJazz({
      app,
      permissions,
      store: vercel({
        namespace: legacyNamespace,
        region: "iad1",
        sdk: fake.sdk,
        backendCredential: core.credential,
      }),
    }).ready();
    cleanups.push(() => jazz.close());
    const row = await jazz.db.insert(app.notes, { text: "kept" }).wait({ tier: "global" });

    // The Sandbox stops under the backend's live session, and resuming it
    // takes longer than two handshake timeouts (reconnects used to give up
    // while they waited for the wake; since #6294 they wait it out). Then the
    // woken core is busy for a moment, so the first connection to it times out.
    const get = fake.sdk.Sandbox.get;
    fake.sdk.Sandbox.get = async (params) => {
      if (params.resume) await sleep(25_000);
      return get(params);
    };
    fake.state.onStart = () => {
      busyUntil = Date.now() + 12_000;
    };
    fake.state.status = "stopped";
    for (const socket of passing) socket.destroy();
    fake.calls.length = 0;

    // Reads fail through the outage, then succeed once the busy core
    // answers, without this instance stopping the session it woke.
    const deadline = Date.now() + 90_000;
    let seen: string[] | undefined;
    while (!seen && Date.now() < deadline) {
      try {
        const notes = await jazz.db.all(app.notes.where({ id: row.id }));
        seen = notes.map((note) => note.text);
      } catch {
        await sleep(1_000);
      }
    }
    expect(seen).toEqual(["kept"]);
    expect(fake.calls.filter((call) => !call.startsWith("extend"))).toEqual([
      "resume jazz-prj-test-production",
      expect.stringMatching(/^start /),
    ]);
  }, 120_000);
});
