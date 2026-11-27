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

describe("vercel() store: a session connections stop reaching", () => {
  it("starts a fresh session when connections stop reaching a core that still answers /health", async () => {
    // Vercel's router in front of the core. While `cut`, it passes plain
    // HTTP (so `/health` answers) but holds every WebSocket upgrade without
    // an answer, and it has dropped the connections it was passing: the
    // core runs on, unreached. A fresh session routes again.
    const core = await startTrustedCore(randomUUID());
    const target = new URL(core.url);
    let cut = false;
    const passing = new Set<Socket>();
    const router = createNetServer((client) => {
      client.once("data", (head) => {
        if (cut && /^upgrade:\s*websocket/im.test(head.toString("latin1"))) {
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
        client.on("error", end).on("close", end);
        upstream.on("error", end).on("close", end);
      });
    });
    await new Promise<void>((resolve) => router.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => router.close(resolve)));
    const url = `http://127.0.0.1:${(router.address() as AddressInfo).port}`;
    const fake = fakeVercel({ appId: core.appId, url });
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });
    const jazz = await createJazz({
      app,
      permissions,
      store: vercel({
        blobs: "sqlite",
        namespace: legacyNamespace,
        region: "iad1",
        sdk: fake.sdk,
        backendCredential: core.credential,
      }),
    }).ready();
    cleanups.push(() => jazz.close());
    const row = await jazz.db.insert(app.notes, { text: "kept" }).wait({ tier: "global" });

    cut = true;
    for (const socket of passing) socket.destroy();
    fake.state.onStart = () => {
      cut = false;
    };
    fake.calls.length = 0;

    // Reads fail while nothing gets through, then succeed once the
    // instance has stopped the session no connection reaches and woken a
    // fresh one, without anything else intervening.
    const deadline = Date.now() + 90_000;
    let seen: string[] | undefined;
    while (!seen && Date.now() < deadline) {
      try {
        const notes = await jazz.db.all(app.notes.where({ id: row.id }), { tier: "remote" });
        seen = notes.map((note) => note.text);
      } catch {
        await sleep(500);
      }
    }
    expect(seen).toEqual(["kept"]);
    expect(fake.calls.filter((call) => !call.startsWith("extend"))).toEqual([
      "stop",
      "resume jazz-prj-test-production",
      expect.stringMatching(/^start /),
    ]);
  }, 120_000);
});
