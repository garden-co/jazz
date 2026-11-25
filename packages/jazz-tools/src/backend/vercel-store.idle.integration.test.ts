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

/** How long a test watches a backend that has no requests of its own. */
const QUIET_MS = 15_000;

/**
 * A backend on a fake Vercel whose router, like Vercel's, answers
 * `410 SANDBOX_STOPPED` while the Sandbox isn't running and passes
 * connections to the core while it is.
 */
async function backendOnVercel() {
  const core = await startTrustedCore(randomUUID());
  const target = new URL(core.url);
  let fake!: ReturnType<typeof fakeVercel>;
  const passing = new Set<Socket>();
  const router = createNetServer((client) => {
    client.on("error", () => {});
    client.once("data", (head) => {
      if (fake.state.status !== "running" || !fake.state.exists) {
        client.end(
          "HTTP/1.1 410 Gone\r\nx-vercel-error: SANDBOX_STOPPED\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        );
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
  /** Drop the connections the router passes, as a stopping session does. */
  const cut = () => {
    for (const socket of passing) socket.destroy();
  };
  return { jazz, fake, row, cut };
}

/** What the backend asked of Vercel, but the keepalive's extensions. */
const asked = (fake: ReturnType<typeof fakeVercel>) =>
  fake.calls.filter((call) => !call.startsWith("extend"));

describe("vercel() store: a backend with no requests of its own (#6372)", () => {
  it("leaves its core stopped once it stops, until a request needs it", async () => {
    const { jazz, fake, row, cut } = await backendOnVercel();

    // The core stops under the backend's live session (its idle timeout, or
    // `vercel sandbox stop`), and nothing asks this backend for anything.
    fake.state.status = "stopped";
    cut();
    fake.calls.length = 0;
    await sleep(QUIET_MS);
    expect(asked(fake)).toEqual([]);
    expect(fake.state.status).toBe("stopped");

    // A request that needs the core wakes it, and is answered.
    const seen = await jazz.db.all(app.notes.where({ id: row.id }), { tier: "remote" });
    expect(seen.map((note) => note.text)).toEqual(["kept"]);
    expect(asked(fake)).toEqual([
      "resume jazz-prj-test-production",
      expect.stringMatching(/^start /),
    ]);
  }, 60_000);

  it("never recreates its core, or its Drive, after both are deleted", async () => {
    const { fake, cut } = await backendOnVercel();

    // `vercel sandbox rm` and `vercel sandbox drives rm`: a fresh Sandbox
    // and Drive by the same name would be an empty store.
    fake.state.exists = false;
    fake.state.status = "stopped";
    fake.state.drive = false;
    fake.files.delete("/data/app-id");
    cut();
    fake.calls.length = 0;
    await sleep(QUIET_MS);
    expect(asked(fake)).toEqual([]);
    expect(fake.state.exists).toBe(false);
    expect(fake.state.drive).toBe(false);
  }, 60_000);
});
