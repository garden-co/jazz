import { describe, expect, it, vi } from "vitest";
import { randomUUID } from "node:crypto";
import {
  connect as connectTcp,
  createServer as createNetServer,
  type AddressInfo,
  type Server,
} from "node:net";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { createJazz, vercel } from "./index.js";
import {
  app,
  AUDIENCE,
  cleanups,
  fakeVercel,
  legacyNamespace,
  oidcToken,
  onVercel,
  permissions,
  runCleanupsAfterEach,
  startIssuer,
  startTrustedCore,
  SUBJECT,
} from "./fixtures/vercel-store.js";

runCleanupsAfterEach();

/**
 * Vercel's router in front of a core: while `target()` names no port the
 * core listens on, it answers as Vercel's does for a Sandbox whose port
 * nothing listens on (or for the session it can't route, `refuse()`), and
 * otherwise passes the connection through.
 */
async function startRouter(target: () => number | undefined, refuse: () => boolean = () => false) {
  const router: Server = createNetServer((client) => {
    client.on("error", () => {});
    client.once("data", (head) => {
      const port = target();
      if (port === undefined || refuse()) {
        client.end(
          "HTTP/1.1 502 Bad Gateway\r\nx-vercel-error: SANDBOX_NOT_LISTENING\r\n" +
            "content-length: 21\r\nconnection: close\r\n\r\nSANDBOX_NOT_LISTENING",
        );
        return;
      }
      const upstream = connectTcp(port, "127.0.0.1", () => {
        upstream.write(head);
        client.pipe(upstream).pipe(client);
      });
      const end = () => {
        client.destroy();
        upstream.destroy();
      };
      client.on("close", end);
      upstream.on("error", end).on("close", end);
    });
  });
  await new Promise<void>((resolve) => router.listen(0, "127.0.0.1", resolve));
  cleanups.push(() => new Promise((resolve) => router.close(resolve)));
  return `http://127.0.0.1:${(router.address() as AddressInfo).port}`;
}

const instance = (fake: ReturnType<typeof fakeVercel>, credential: () => string) =>
  createJazz({
    app,
    permissions,
    store: vercel({
      namespace: legacyNamespace,
      region: "iad1",
      sdk: fake.sdk,
      backendCredential: credential,
    }),
  }).ready();

describe("vercel() store: a core that takes a while to open", () => {
  it("waits for a core whose open outlasts the recycle timeout, from several instances at once, without stopping it", async () => {
    // A core with a large store: once started, it opens for 20 s (longer
    // than the 12 s after which a session the router doesn't reach is
    // recycled) before it listens. Stopping the session kills the open.
    const OPEN_MS = 20_000;
    const appId = randomUUID();
    const issuer = await startIssuer();
    let opening: ReturnType<typeof setTimeout> | undefined;
    let launchedAt = 0;
    let core: { port: number; stop: () => Promise<void> } | undefined;
    const router = await startRouter(() => core?.port);
    const fake = fakeVercel({ appId, url: router });
    fake.state.onStart = () => {
      if (opening || core) return;
      launchedAt = Date.now();
      opening = setTimeout(async () => {
        const started = await startLocalJazzServer({
          appId,
          inMemory: true,
          trustedWorkload: { issuer, audience: AUDIENCE, subject: SUBJECT },
        });
        cleanups.push(() => started.stop());
        core = { port: Number(new URL(started.url).port), stop: () => started.stop() };
      }, OPEN_MS);
    };
    fake.state.onStop = () => {
      clearTimeout(opening);
      opening = undefined;
      void core?.stop();
      core = undefined;
    };
    cleanups.push(async () => clearTimeout(opening));
    // What the Sandbox's own check sees: the core's process runs from its
    // launch, and listens once its open is done.
    fake.state.coreState = () => {
      const up = Math.round((Date.now() - launchedAt) / 1000);
      return core ? `listening ${up}` : opening ? `opening ${up}` : "down";
    };
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    cleanups.push(async () => info.mockRestore());
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });

    // Three function instances of the deployment, each with its own store,
    // wake the core at once, each with a request of its own waiting too.
    const credential = () => oidcToken(issuer);
    const instances = await Promise.all([1, 2, 3].map(() => instance(fake, credential)));
    for (const jazz of instances) cleanups.push(() => jazz.close());
    const rows = await Promise.all(
      instances.map((jazz, i) =>
        jazz.db.insert(app.notes, { text: `from ${i}` }).wait({ tier: "global" }),
      ),
    );
    const seen = await instances[0]!.db.all(app.notes, { tier: "remote" });
    expect(seen.map((note) => note.id).sort()).toEqual(rows.map((row) => row.id).sort());

    // One session, never stopped: the core opened once.
    expect(fake.calls).not.toContain("stop");
    expect(fake.recycleClaims).toEqual([]);
    expect(Date.now() - launchedAt).toBeGreaterThanOrEqual(OPEN_MS);
    // Each instance said why it waited.
    expect(info).toHaveBeenCalledWith(
      expect.stringMatching(/core jazz-prj-test-production is opening its store/),
    );
    expect(info).toHaveBeenCalledWith(
      expect.stringMatching(/woke in \d+ ms \(.*waited for its core to open its store/),
    );
  }, 90_000);

  it("says the core is still opening when a wake gives up on it", async () => {
    // The open outlasts the wake's deadline: the wake says so, and leaves
    // the core to finish opening.
    const appId = randomUUID();
    const issuer = await startIssuer();
    const router = await startRouter(() => undefined);
    const fake = fakeVercel({ appId, url: router });
    const launchedAt = Date.now();
    fake.state.coreState = () => `opening ${Math.round((Date.now() - launchedAt) / 1000)}`;
    vi.spyOn(console, "info").mockImplementation(() => {});
    cleanups.push(async () => vi.restoreAllMocks());
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });

    await expect(instance(fake, () => oidcToken(issuer))).rejects.toThrow(
      /core jazz-prj-test-production is still opening its store: its process launched \d+ s ago and doesn't listen yet; gave up waiting after \d+ ms, and the core keeps opening/,
    );
    expect(fake.calls).not.toContain("stop");
  }, 90_000);

  it("recycles a session the router doesn't reach while its core listens, exactly once for all instances", async () => {
    // The core listens, but Vercel's router answers SANDBOX_NOT_LISTENING
    // for the first session it resumes in; a fresh session routes.
    const core = await startTrustedCore(randomUUID());
    const port = Number(new URL(core.url).port);
    let glitched: Date | undefined;
    let routed!: ReturnType<typeof fakeVercel>;
    const router = await startRouter(
      () => port,
      () => routed.state.startedAt === glitched,
    );
    routed = fakeVercel({ appId: core.appId, url: router });
    routed.state.onStart = () => {
      glitched ??= routed.state.startedAt;
    };
    Object.assign(routed.state, { exists: true, drive: true, status: "stopped" });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    cleanups.push(async () => vi.restoreAllMocks());
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });

    const instances = await Promise.all([1, 2, 3].map(() => instance(routed, core.credential)));
    for (const jazz of instances) cleanups.push(() => jazz.close());
    const row = await instances[1]!.db.insert(app.notes, { text: "routed" }).wait({
      tier: "global",
    });
    const seen = await instances[2]!.db.all(app.notes.where({ id: row.id }), { tier: "remote" });
    expect(seen.map((note) => note.text)).toEqual(["routed"]);

    // The glitched session was stopped once, by the one waiter that
    // claimed it; every instance came up on the next.
    expect(routed.calls.filter((call) => call === "stop")).toHaveLength(1);
    expect(routed.recycleClaims).toEqual([glitched!.getTime()]);
    expect(routed.state.startedAt).not.toBe(glitched);
    expect(warn.mock.calls.filter(([message]) => /stopping core/.test(String(message)))).toEqual([
      [
        expect.stringMatching(
          /stopping core jazz-prj-test-production's session, whose core listens, so the next wake starts a fresh one: Vercel's router answered SANDBOX_NOT_LISTENING for \d+ ms while it listened/,
        ),
      ],
    ]);
  }, 90_000);
});
