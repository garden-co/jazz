import { createHmac, randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { onVercelHost } from "./vercel-host.js";
import {
  connect as connectTcp,
  createServer as createNetServer,
  type AddressInfo,
  type Socket,
} from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { testApp } from "../testing/index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { fetchFor } from "../runtime/in-process-transport.js";
import { createJazz, memory, vercel, vercelBlob } from "./index.js";
import { HANDSHAKE_TIMEOUT_MS } from "../runtime/native-runtime/websocket.js";
import {
  newerVersion,
  perBranch,
  openVercelCore,
  onVercelDeployment,
  refuseOtherEnvironment,
  startScript,
  workloadOf,
  type SandboxHandle,
  type SandboxSdk,
} from "./vercel-store.js";

const app = testApp({ tables: { notes: { columns: { text: "string" } } } });
// Updates and deletes are denied.
const permissions = { format: 1, tables: { notes: { read: "true", insert: "true" } } };

// The cores earlier versions created by default, production's and one per
// preview branch (see vercel-namespace.test.ts).
const legacyNamespace = ({ environment, branch }: { environment: string; branch?: string }) =>
  environment === "production" ? "production" : perBranch(branch);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

const SIGNING_KEY = "synthetic-vercel-oidc-signing-key-0123456789";
const AUDIENCE = "https://vercel.com/garden-co";
const SUBJECT = "owner:garden-co:project:app:environment:production";

/** A stand-in for Vercel's OIDC issuer: discovery plus one HS256 key. */
async function startIssuer(): Promise<string> {
  const server = createServer((request, response) => {
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    response.setHeader("content-type", "application/json");
    if (request.url === "/.well-known/openid-configuration") {
      response.end(JSON.stringify({ issuer: base, jwks_uri: `${base}/jwks` }));
    } else {
      const k = Buffer.from(SIGNING_KEY).toString("base64url");
      response.end(JSON.stringify({ keys: [{ kty: "oct", kid: "v1", alg: "HS256", k }] }));
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(() => new Promise((resolve) => server.close(resolve)));
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

/** A function's OIDC token, as `getVercelOidcToken()` returns it. */
function oidcToken(issuer: string, subject = SUBJECT): string {
  const part = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const exp = Math.floor(Date.now() / 1000) + 600;
  const unsigned = `${part({ alg: "HS256", kid: "v1", typ: "JWT" })}.${part({ iss: issuer, aud: AUDIENCE, sub: subject, exp })}`;
  const signature = createHmac("sha256", SIGNING_KEY).update(unsigned).digest("base64url");
  return `${unsigned}.${signature}`;
}

/** The core as the start script runs it: trusting only the function's workload. */
async function startTrustedCore(appId: string) {
  const issuer = await startIssuer();
  const core = await startLocalJazzServer({
    appId,
    inMemory: true,
    trustedWorkload: { issuer, audience: AUDIENCE, subject: SUBJECT },
  });
  cleanups.push(() => core.stop());
  return { appId, url: core.url, issuer, credential: () => oidcToken(issuer) };
}

/**
 * `@vercel/sandbox` standing in for Vercel: one Sandbox whose port routes to a
 * local jazz-tools server, and whose Drive holds that server's app id.
 */
function fakeVercel(core: { appId: string; url: string }) {
  const calls: string[] = [];
  const state = {
    exists: false,
    status: "stopped",
    drive: false,
    /** The Sandbox's region, and its Drive's: set when each is created. */
    region: "iad1",
    driveRegion: "iad1",
    startedAt: new Date(),
    timeout: 300_000,
    /** What the core runs; a start script switches it to its own version. */
    running: undefined as string | undefined,
    /** The next `times` starts give up like the start script's `fail`. */
    failStarts: undefined as
      | { kind: "permanent" | "transient"; why: string; times: number }
      | undefined,
    /** Whether the last start gave up (its core then listens nowhere). */
    failing: false,
    /** The next `slowStarts` starts haven't run yet when the function looks. */
    slowStarts: 0,
    /**
     * The next `failLookups` lookups of the Sandbox (not resumes) fail as
     * Vercel's API does when it is briefly unavailable.
     */
    failLookups: 0,
    /** Called when a start brings the core up. */
    onStart: undefined as (() => void) | undefined,
  };
  const files = new Map<string, Buffer>([["/data/app-id", Buffer.from(`${core.appId}\n`)]]);
  const handle = (): SandboxHandle => ({
    get status() {
      return state.status;
    },
    get region() {
      return state.region;
    },
    domain: () => (state.failing ? "http://127.0.0.1:9" : core.url),
    currentSession: () => ({
      startedAt: state.startedAt,
      timeout: state.timeout,
      extendTimeout: async (ms: number) => {
        calls.push(`extend ${ms}`);
        state.timeout += ms;
      },
    }),
    runCommand: async ({ args }) => {
      const scriptPath = /exec bash (\S+)/.exec(args.at(-1)!)![1]!;
      const script = files.get(scriptPath)!.toString();
      const startId = scriptPath.replace(/^.*\//, "").replace(/\.sh$/, "");
      const version = /^version='(.*)'$/m.exec(script)![1]!;
      // Like the real script: the first start pins the trust, others don't move it.
      const trust = /^trust='(.*)'$/m.exec(script)![1]!;
      const pinned = files.get("/data/trust")?.toString().trim();
      if (pinned && pinned !== trust) return;
      files.set("/data/trust", Buffer.from(`${trust}\n`));
      calls.push(`start ${version}`);
      if (state.slowStarts > 0) {
        state.slowStarts--;
        return;
      }
      const errorFile = `/tmp/jazz-${version}.error`;
      files.delete(errorFile);
      if (state.failStarts && state.failStarts.times > 0) {
        state.failStarts.times--;
        state.failing = true;
        files.set(
          errorFile,
          Buffer.from(`${startId} ${state.failStarts.kind}: ${state.failStarts.why}\n`),
        );
        return;
      }
      state.failing = false;
      state.onStart?.();
      if (!state.running || newerVersion(version, state.running)) state.running = version;
    },
    writeFiles: async (written) => {
      for (const file of written) files.set(file.path, file.content);
    },
    readFileToBuffer: async ({ path }) =>
      path === "/tmp/jazz.version"
        ? state.running
          ? Buffer.from(`${state.running}\n`)
          : null
        : (files.get(path) ?? null),
    stop: async () => {
      calls.push("stop");
      state.status = "stopped";
    },
  });
  const sdk: SandboxSdk = {
    Sandbox: {
      async get(params) {
        if (!params.resume && state.failLookups > 0) {
          state.failLookups--;
          calls.push("lookup failed");
          throw Object.assign(new Error("Status code 503 is not ok"), {
            response: { status: 503 },
          });
        }
        if (!state.exists) throw new Error("404 sandbox not found");
        if (params.resume) {
          calls.push(`resume ${String(params.name)}`);
          state.status = "running";
          const sbx = handle();
          await (params.onResume as (sbx: SandboxHandle) => Promise<void>)(sbx);
          return sbx;
        }
        return handle();
      },
      async create(params) {
        if (!state.drive) throw new Error("drive missing");
        calls.push(`create ${String(params.name)} in ${String(params.region)}`);
        state.region = String(params.region);
        state.exists = true;
        state.status = "running";
        return handle();
      },
    },
    Drive: {
      async get() {
        if (!state.drive) throw new Error("404 drive not found");
        return { region: state.driveRegion };
      },
      async getOrCreate(params) {
        calls.push(`drive ${String(params.name)} in ${String(params.region)}`);
        // An existing Drive keeps its region, as Vercel's do.
        if (!state.drive) state.driveRegion = String(params.region);
        state.drive = true;
        return { region: state.driveRegion };
      },
    },
  };
  return { sdk, calls, state, files };
}

function onVercel(env: Record<string, string>) {
  const before = {
    VERCEL: process.env.VERCEL,
    VERCEL_ENV: process.env.VERCEL_ENV,
    VERCEL_PROJECT_ID: process.env.VERCEL_PROJECT_ID,
    VERCEL_GIT_COMMIT_REF: process.env.VERCEL_GIT_COMMIT_REF,
  };
  Object.assign(process.env, env);
  cleanups.push(async () => {
    for (const [key, value] of Object.entries(before)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });
}

describe("vercel() store", () => {
  it("creates the app's Sandbox and Drive on first use and serves through it", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    onVercel({
      VERCEL: "1",
      VERCEL_ENV: "preview",
      VERCEL_PROJECT_ID: "prj_test",
      VERCEL_GIT_COMMIT_REF: "main",
    });

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
    const name = `jazz-prj-test-${perBranch("main")}`;
    expect(fake.calls.slice(0, 2)).toEqual([
      `drive ${name}-data in iad1`,
      `create ${name} in iad1`,
    ]);
    expect(fake.calls[2]).toMatch(/^start /);
    const scripts = [...fake.files.keys()].filter((path) => path.startsWith("/tmp/jazz-start-"));
    const script = fake.files.get(scripts[0]!)?.toString();
    // No app id is named: the core keeps the one on its Drive.
    expect(script).toContain("app_id=$(cat /proc/sys/kernel/random/uuid)");
    expect(jazz.appId).toBe(appId);
    // The core trusts exactly the workload the function's own token names.
    expect(script).toContain(
      JSON.stringify({ issuer: core.issuer, audience: AUDIENCE, subject: SUBJECT }),
    );

    // The backend's own database reaches the core in the Sandbox.
    const row = await jazz.db.insert(app.notes, { text: "on vercel" }).wait({ tier: "global" });
    // Another function instance (here: after this one's Jazz closed, or the
    // same Jazz would answer) reaches the same core and its data.
    await jazz.close();
    const again = await createJazz({
      app,
      permissions,
      store: vercel({
        namespace: legacyNamespace,
        region: "iad1",
        sdk: fake.sdk,
        backendCredential: core.credential,
      }),
    }).ready();
    cleanups.push(() => again.close());
    const seen = await again.db.all(app.notes.where({ id: row.id }));
    expect(seen.map((note) => note.text)).toEqual(["on vercel"]);
  }, 60_000);

  it("refreshes credentials on reuse and refuses changes to the live Sandbox configuration", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const { sdk } = fakeVercel({ ...core, appId });
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });
    const options = { namespace: "test", region: "iad1", sdk, backendCredential: core.credential };
    const jazz = await createJazz({ app, permissions, store: vercel(options) }).ready();
    cleanups.push(() => jazz.close());

    const refreshed = vi.fn(core.credential);
    expect(
      createJazz({ app, permissions, store: vercel({ ...options, backendCredential: refreshed }) }),
    ).toBe(jazz);
    expect(() =>
      createJazz({ app, permissions, store: vercel({ ...options, region: "fra1" }) }),
    ).toThrow(/other store settings/);
    await jazz.createAccountFor("alice");
    expect(refreshed).toHaveBeenCalled();
  }, 60_000);

  it("reaches a core only with the trusted workload's token", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });
    const preview = () => oidcToken(core.issuer, "owner:garden-co:project:app:environment:preview");

    // The core itself refuses another workload's token.
    const fresh = fakeVercel(core);
    Object.assign(fresh.state, { exists: true, drive: true });
    await expect(
      createJazz({
        app,
        permissions,
        store: vercel({
          namespace: legacyNamespace,
          region: "iad1",
          sdk: fresh.sdk,
          backendCredential: preview,
        }),
      }).ready(),
    ).rejects.toThrow(/401/);

    // No secret gets past it either: only the token's stated access counts.
    const guessed = await fetch(`${core.url}/apps/${appId}/admin/bundles`, {
      headers: { "X-Jazz-Admin-Secret": "stated-by-backend-credential" },
    });
    expect(guessed.status).toBe(401);
  }, 60_000);

  it("never retargets a core to another environment's workload", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel(core);
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
    const pinned = fake.files.get("/data/trust")?.toString();
    expect(pinned).toContain(SUBJECT);

    // A preview deployment (another process: this one's Jazz closes first)
    // that reaches the production Sandbox (a shared name) finds it pinned,
    // and its start leaves the pin alone.
    await jazz.close();
    const preview = () => oidcToken(core.issuer, "owner:garden-co:project:app:environment:preview");
    await expect(
      createJazz({
        app,
        permissions,
        store: vercel({
          namespace: legacyNamespace,
          region: "iad1",
          sdk: fake.sdk,
          backendCredential: preview,
        }),
      }).ready(),
    ).rejects.toThrow(/trusts .*environment:production, not this deployment/);
    expect(fake.files.get("/data/trust")?.toString()).toBe(pinned);
  }, 60_000);

  it("refuses a stopped core pinned to another workload before waking it up", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel(core);
    Object.assign(fake.state, { exists: true, drive: true });
    fake.files.set("/data/trust", Buffer.from(`${core.issuer} ${AUDIENCE} ${SUBJECT}\n`));
    onVercel({
      VERCEL: "1",
      VERCEL_ENV: "preview",
      VERCEL_PROJECT_ID: "prj_test",
      VERCEL_GIT_COMMIT_REF: "main",
    });
    const preview = () => oidcToken(core.issuer, "owner:garden-co:project:app:environment:preview");
    const started = Date.now();
    await expect(
      createJazz({
        app,
        permissions,
        store: vercel({
          namespace: legacyNamespace,
          region: "iad1",
          sdk: fake.sdk,
          backendCredential: preview,
        }),
      }).ready(),
    ).rejects.toThrow(/delete \/data\/trust/);
    expect(Date.now() - started).toBeLessThan(5_000);
  }, 60_000);

  it("refuses a token for another environment than the deployment's", async () => {
    onVercel({
      VERCEL: "1",
      VERCEL_ENV: "preview",
      VERCEL_PROJECT_ID: "prj_test",
      VERCEL_GIT_COMMIT_REF: "main",
    });
    const fake = fakeVercel({ appId: randomUUID(), url: "http://unused.invalid" });
    process.env.VERCEL_OIDC_TOKEN = oidcToken("https://oidc.vercel.com/garden-co");
    cleanups.push(async () => {
      delete process.env.VERCEL_OIDC_TOKEN;
    });
    await expect(
      createJazz({
        app,
        permissions,
        store: vercel({ namespace: legacyNamespace, region: "iad1", sdk: fake.sdk }),
      }).ready(),
    ).rejects.toThrow(/preview deployment's OIDC token is for .*environment:production/);
    expect(fake.calls).toEqual([]);
  }, 60_000);

  it("sends a request Vercel's router refused again once the core is back", async () => {
    // Vercel's router in front of the Sandbox's port: while the core is down
    // it answers itself, with its error header.
    const router = { down: false, served: 0 };
    const server = createServer((request, response) => {
      if (router.down) {
        response.writeHead(502, { "x-vercel-error": "SANDBOX_NOT_LISTENING" });
        return response.end("SANDBOX_NOT_LISTENING");
      }
      if (request.url !== "/health") router.served++;
      response.end("ok");
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => server.close(resolve)));
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const appId = randomUUID();
    const fake = fakeVercel({ appId, url });
    // A start brings the core back up.
    fake.state.onStart = () => {
      router.down = false;
    };
    const credential = oidcToken(await startIssuer());
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: () => credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url: base }) =>
        ({
          serverUrl: base,
          async handle(method: string, path: string) {
            const response = await fetch(`${base}${path}`, { method });
            const headers: string[][] = [];
            response.headers.forEach((value, name) => headers.push([name, value]));
            return {
              status: response.status,
              headers,
              body: new Uint8Array(await response.arrayBuffer()),
            };
          },
          close: async () => {},
        }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());

    // The core stops while the Sandbox still runs, inside the window in
    // which this instance trusts it without a check.
    router.down = true;
    const starts = fake.calls.filter((call) => call.startsWith("start")).length;
    const answer = await opened.handle("GET", "/apps/x/schemas", [], new Uint8Array());
    expect(answer.status).toBe(200);
    expect(fake.calls.filter((call) => call.startsWith("start")).length).toBe(starts + 1);
    expect(router.served).toBe(1);

    // The backend's own requests (in-process) are sent again too.
    router.down = true;
    const response = await fetchFor(opened.serverUrl)(`${opened.serverUrl}/apps/x/schemas`);
    expect(response.status).toBe(200);
    expect(router.served).toBe(2);

    // A small buffered body is sent again as well.
    router.down = true;
    const posted = await fetchFor(opened.serverUrl)(`${opened.serverUrl}/apps/x/schemas`, {
      method: "POST",
      body: new Uint8Array(1024),
    });
    expect(posted.status).toBe(200);
    expect(router.served).toBe(3);
  }, 60_000);

  it("never holds a streamed upload to send it again: the router's answer passes, and the core wakes for the next request", async () => {
    const router = { down: false, served: 0 };
    const server = createServer((request, response) => {
      request.resume();
      if (router.down) {
        response.writeHead(502, { "x-vercel-error": "SANDBOX_NOT_LISTENING" });
        return response.end("SANDBOX_NOT_LISTENING");
      }
      if (request.url !== "/health") router.served++;
      response.end("ok");
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => server.close(resolve)));
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const fake = fakeVercel({ appId: randomUUID(), url });
    fake.state.onStart = () => {
      router.down = false;
    };
    const credential = oidcToken(await startIssuer());
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: () => credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url: base }) =>
        ({ serverUrl: base, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());

    router.down = true;
    const clone = vi.spyOn(Request.prototype, "clone");
    const upload = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(64 * 1024));
        controller.close();
      },
    });
    const response = await fetchFor(opened.serverUrl)(`${opened.serverUrl}/apps/x/upload`, {
      method: "POST",
      body: upload,
      duplex: "half",
    } as RequestInit);
    expect(clone).not.toHaveBeenCalled();
    expect(response.status).toBe(502);
    // The core is woken for the next request, which goes through.
    const starts = fake.calls.filter((call) => call.startsWith("start")).length;
    const next = await fetchFor(opened.serverUrl)(`${opened.serverUrl}/apps/x/schemas`);
    expect(next.status).toBe(200);
    expect(fake.calls.filter((call) => call.startsWith("start")).length).toBe(starts + 1);
  }, 60_000);

  it("stops the Sandbox of a core that can't start, and holds off retrying", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: core.credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());

    // The core stops (idle), and its next start can never succeed.
    fake.state.status = "stopped";
    fake.state.failStarts = {
      kind: "permanent",
      why: "could not download the core for 1.2.3 from https://registry.example/core.tgz (HTTP 404)",
      times: Infinity,
    };
    // Past the window in which a wake is trusted without a check.
    await new Promise((resolve) => setTimeout(resolve, 10_500));
    const t0 = Date.now();
    await expect(opened.handle("GET", "/health", [], new Uint8Array())).rejects.toThrow(
      /core jazz-prj-test-test could not start: could not download the core for 1\.2\.3 .*HTTP 404/,
    );
    // Reported within one health round, not after the minute-long wake deadline,
    // and the Sandbox isn't kept awake.
    expect(Date.now() - t0).toBeLessThan(15_000);
    expect(fake.state.status).toBe("stopped");
    // The next request fails at once, without waking the Sandbox again.
    const calls = fake.calls.length;
    await expect(opened.handle("GET", "/health", [], new Uint8Array())).rejects.toThrow(
      /could not start/,
    );
    expect(fake.calls.length).toBe(calls);
  }, 60_000);

  it("refuses a Blob store another environment claimed, before serving", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    const asked: string[] = [];
    // The test core's workload is production's; the store says preview's.
    // Answers at once, before the wake is done.
    const claimedByPreview = (async (input: RequestInfo | URL) => {
      asked.push(String(input));
      return new Response("preview\n");
    }) as typeof fetch;
    let closed = 0;
    await expect(
      openVercelCore({
        options: {
          sdk: fake.sdk,
          backendCredential: core.credential,
          namespace: "test",
          region: "iad1",
          blobs: vercelBlob({ storeId: "store_Shared1" }),
        },
        openRemote: ({ url }) =>
          ({
            serverUrl: url,
            handle: async () => ({}),
            close: async () => {
              closed++;
            },
          }) as never,
        version: "1.2.3",
        env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
        fetch: claimedByPreview,
      }),
    ).rejects.toThrow(/Blob store Shared1 serves the preview environment, not production/);
    // The core the wake came up with is closed, though the refusal came first.
    for (let i = 0; closed === 0 && i < 300; i++) await new Promise((r) => setTimeout(r, 100));
    expect(closed).toBe(1);
    expect(asked).toEqual([
      "https://shared1.private.blob.vercel-storage.com/.jazz-environment?cache=0",
    ]);
  }, 60_000);

  it("never reads an error file it didn't cause as its start's failure", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    // Left by an earlier session's start (say, before the URL was fixed),
    // and not yet removed by this session's start script.
    fake.files.set(
      "/tmp/jazz-1.2.3.error",
      Buffer.from("jazz-start-earlier permanent: could not download the core (HTTP 404)\n"),
    );
    fake.state.failing = true;
    // This session's first starts come up slowly: the stale file is still
    // there while the core isn't listening yet.
    fake.state.slowStarts = 2;
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: core.credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());
    expect(fake.calls).not.toContain("stop");
  }, 60_000);

  it("creates a new core's Sandbox and Drive in the configured region", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: core.credential,
        namespace: "test",
        region: "fra1",
      },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());
    expect(fake.calls.slice(0, 2)).toEqual([
      "drive jazz-prj-test-test-data in fra1",
      "create jazz-prj-test-test in fra1",
    ]);
  }, 60_000);

  it('keeps an existing iad1 core, which earlier versions created by default, with region: "iad1"', async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    Object.assign(fake.state, { exists: true, drive: true, region: "iad1", driveRegion: "iad1" });
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: core.credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());
    expect(fake.calls).toContain("resume jazz-prj-test-test");
    expect(fake.calls.filter((call) => /^(create|drive) /.test(call))).toEqual([]);
  }, 60_000);

  it("refuses an existing core in another region instead of silently using it", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    for (const status of ["stopped", "running"]) {
      const fake = fakeVercel({ ...core, appId });
      Object.assign(fake.state, { exists: true, drive: true, status, region: "iad1" });
      await expect(
        openVercelCore({
          options: {
            sdk: fake.sdk,
            backendCredential: core.credential,
            namespace: "test",
            region: "fra1",
          },
          openRemote: ({ url }) =>
            ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
          version: "1.2.3",
          env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
        }),
      ).rejects.toThrow(
        /core jazz-prj-test-test is in iad1, but `region` is "fra1"\. Moving a core to another region isn't supported yet.*issues\/5441.*pass `region: "iad1"`/,
      );
      // Nothing was created or started in either region.
      expect(fake.calls.filter((call) => /^(create|drive|start) /.test(call))).toEqual([]);
    }
  }, 60_000);

  it("never reads a failed lookup of an existing core as a missing one", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const open = (sdk: SandboxSdk, region: string) =>
      openVercelCore({
        options: { sdk, backendCredential: core.credential, namespace: "test", region },
        openRemote: ({ url }) =>
          ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
        version: "1.2.3",
        env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
      });

    // An iad1 core, opened with the wrong region while Vercel's API answers
    // 503 to the running check and the wake's lookup: the lookup is retried
    // and the core refused, never provisioned again in fra1.
    const wrong = fakeVercel({ ...core, appId });
    Object.assign(wrong.state, { exists: true, drive: true, region: "iad1", failLookups: 2 });
    await expect(open(wrong.sdk, "fra1")).rejects.toThrow(
      /core jazz-prj-test-test is in iad1, but `region` is "fra1"/,
    );
    expect(wrong.calls.filter((call) => call === "lookup failed")).toHaveLength(2);
    expect(wrong.calls.filter((call) => /^(create|drive|start) /.test(call))).toEqual([]);

    // With the right region, the same failures delay the wake, nothing more.
    const right = fakeVercel({ ...core, appId });
    Object.assign(right.state, { exists: true, drive: true, region: "iad1", failLookups: 2 });
    const opened = await open(right.sdk, "iad1");
    cleanups.push(() => opened.close());
    expect(right.calls).toContain("resume jazz-prj-test-test");
    expect(right.calls.filter((call) => /^(create|drive) /.test(call))).toEqual([]);
  }, 60_000);

  it("refuses a Drive left in another region instead of creating a Sandbox that can't mount it", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    // The Sandbox was deleted; its Drive, in iad1, kept the data.
    Object.assign(fake.state, { exists: false, drive: true, driveRegion: "iad1" });
    await expect(
      openVercelCore({
        options: {
          sdk: fake.sdk,
          backendCredential: core.credential,
          namespace: "test",
          region: "fra1",
        },
        openRemote: ({ url }) =>
          ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
        version: "1.2.3",
        env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
      }),
    ).rejects.toThrow(
      /Drive jazz-prj-test-test-data is in iad1, but `region` is "fra1"\. Moving a core to another region isn't supported yet/,
    );
    // Refused before asking for a Drive in fra1, which could create one there.
    expect(fake.calls.filter((call) => /^(create|drive) /.test(call))).toEqual([]);
  }, 60_000);

  it("retries a start that failed for a transient reason, and keeps the Sandbox", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    fake.state.failStarts = {
      kind: "transient",
      why: "could not download the core for 1.2.3 from https://registry.example/core.tgz (fetch failed: ECONNRESET)",
      times: 2,
    };
    const opened = await openVercelCore({
      options: {
        sdk: fake.sdk,
        backendCredential: core.credential,
        namespace: "test",
        region: "iad1",
      },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
    });
    cleanups.push(() => opened.close());
    expect(fake.state.status).toBe("running");
    expect(fake.calls).not.toContain("stop");
    expect(fake.calls.filter((call) => call.startsWith("start ")).length).toBeGreaterThanOrEqual(3);
  }, 60_000);

  it("answers a backend read made while its core restarts, instead of failing it with 1006", async () => {
    // A core that keeps its data on disk, so it can stop and start again
    // on the same address, as a Sandbox's core does across a restart.
    const appId = randomUUID();
    const issuer = await startIssuer();
    const dataDir = await mkdtemp(join(tmpdir(), "jazz-vercel-restart-"));
    cleanups.push(() => rm(dataDir, { recursive: true, force: true }));
    const serve = (port?: number) =>
      startLocalJazzServer({
        appId,
        dataDir,
        port,
        trustedWorkload: { issuer, audience: AUDIENCE, subject: SUBJECT },
      });
    let core = await serve();
    cleanups.push(() => core.stop());
    const port = Number(new URL(core.url).port);
    const fake = fakeVercel({ appId, url: core.url });
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });
    const jazz = await createJazz({
      app,
      permissions,
      store: vercel({
        namespace: legacyNamespace,
        region: "iad1",
        sdk: fake.sdk,
        backendCredential: () => oidcToken(issuer),
      }),
    }).ready();
    cleanups.push(() => jazz.close());
    const row = await jazz.db.insert(app.notes, { text: "kept" }).wait({ tier: "global" });

    // Each restart: the core stops under the backend's live session. While
    // it is down, connections to its address hang for a moment and then
    // fail, as through Vercel's router; the wake that a failed connection
    // triggers starts the core again. A read made while a reconnect is
    // failing waits for the core instead of failing with it.
    const down = () => {
      const held = new Set<Socket>();
      const server = createNetServer((socket) => {
        held.add(socket);
        setTimeout(() => socket.destroy(), 400);
      });
      return {
        listening: new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve)),
        up: async () => {
          for (const socket of held) socket.destroy();
          await new Promise((resolve) => server.close(resolve));
          core = await serve(port);
        },
      };
    };
    let restarting: Promise<unknown> | undefined;
    for (let restart = 0; restart < 3; restart++) {
      restarting = undefined;
      await core.stop();
      const router = down();
      await router.listening;
      fake.state.onStart = () => {
        restarting ??= router.up();
      };
      await sleep(300);
      const seen = await jazz.db.all(app.notes.where({ id: row.id }), { tier: "remote" });
      expect(seen.map((note) => note.text)).toEqual(["kept"]);
      await restarting;
    }
  }, 60_000);
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
  it("answers a backend read made while a wake takes longer than the handshake timeout (#6294)", async () => {
    // Vercel's router in front of the core: while the Sandbox is stopped it
    // answers nothing.
    const core = await startTrustedCore(randomUUID());
    const target = new URL(core.url);
    let fake!: ReturnType<typeof fakeVercel>;
    const passing = new Set<Socket>();
    const router = createNetServer((client) => {
      client.on("error", () => {});
      client.once("data", (head) => {
        if (fake.state.status !== "running") {
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
    // takes longer than the handshake timeout and its confirmation (11 s),
    // well within the wake's own deadline. The backend's reconnect waits for
    // the wake, then connects. (The read itself waits up to 15 s for an
    // answer, a budget of its own: a longer wake fails it as `NotObserved`.)
    const get = fake.sdk.Sandbox.get;
    fake.sdk.Sandbox.get = async (params) => {
      if (params.resume) await sleep(HANDSHAKE_TIMEOUT_MS + 2_000);
      return get(params);
    };
    fake.state.status = "stopped";
    for (const socket of passing) socket.destroy();
    fake.calls.length = 0;

    const seen = await jazz.db.all(app.notes.where({ id: row.id }), { tier: "remote" });
    expect(seen.map((note) => note.text)).toEqual(["kept"]);
    expect(fake.calls.filter((call) => !call.startsWith("extend"))).toEqual([
      "resume jazz-prj-test-production",
      expect.stringMatching(/^start /),
    ]);
  }, 60_000);
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
  it.each([400, 409, 422])(
    "opens every concurrent first request on a cold deploy: losing the race to create the Sandbox waits for the winner's (%i)",
    async (status) => {
      const server = createServer((_request, response) => response.end("ok"));
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      cleanups.push(() => new Promise((resolve) => server.close(resolve)));
      const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      const fake = fakeVercel({ appId: randomUUID(), url });
      // The real API: the name is taken as soon as the first create is
      // accepted, and the Sandbox runs a moment later. A second create gets
      // `@vercel/sandbox`'s APIError (the API's answer and its message).
      const create = fake.sdk.Sandbox.create;
      let creating = false;
      fake.sdk.Sandbox.create = async (params) => {
        if (creating || fake.state.exists) {
          const message = `A sandbox with the name '${String(params.name)}' already exists for this project`;
          throw Object.assign(new Error(`Status code ${status} is not ok: ${message}`), {
            response: new Response(null, { status }),
            json: { error: { code: "bad_request", message } },
          });
        }
        creating = true;
        await sleep(300);
        return create(params);
      };
      const credential = oidcToken(await startIssuer());
      // Separate function instances, each opening the store at once.
      const opened = await Promise.allSettled(
        Array.from({ length: 5 }, () =>
          openVercelCore({
            options: {
              sdk: fake.sdk,
              backendCredential: () => credential,
              namespace: "test",
              region: "iad1",
            },
            openRemote: ({ url: base }) =>
              ({ serverUrl: base, handle: async () => ({}), close: async () => {} }) as never,
            version: "1.2.3",
            env: { VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" },
          }),
        ),
      );
      for (const result of opened) {
        if (result.status === "fulfilled") cleanups.push(() => result.value.close());
      }
      expect(opened.filter((result) => result.status === "rejected")).toEqual([]);
      expect(fake.calls.filter((call) => call.startsWith("create"))).toEqual([
        "create jazz-prj-test-test in iad1",
      ]);
    },
    60_000,
  );

  it("resumes a stopped Sandbox instead of creating another", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    fake.state.exists = true;
    fake.state.drive = true;
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
    expect(fake.calls).toContain("resume jazz-prj-test-production");
    expect(fake.calls.some((call) => call.startsWith("create"))).toBe(false);
  }, 60_000);

  it("logs how long each wake took, and nothing for a core that was already answering", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    fake.state.exists = true;
    fake.state.drive = true;
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });
    const info = vi.spyOn(console, "info");
    cleanups.push(async () => info.mockRestore());
    const wakes = () =>
      info.mock.calls.map(([line]) => String(line)).filter((line) => line.includes(" woke in "));

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
    expect(wakes()).toEqual([
      expect.stringMatching(
        /^jazz: vercel\(\): core jazz-prj-test-production woke in \d+ ms \(resumed\)$/,
      ),
    ]);

    // Another instance finds it running and answering: no wake to report.
    const again = await createJazz({
      app,
      permissions,
      store: vercel({
        namespace: legacyNamespace,
        region: "iad1",
        sdk: fake.sdk,
        backendCredential: core.credential,
      }),
    }).ready();
    cleanups.push(() => again.close());
    expect(wakes()).toHaveLength(1);
  }, 60_000);

  it("refuses another serverless host even with an in-memory local fallback", () => {
    vi.stubEnv("VERCEL", "");
    vi.stubEnv("AWS_LAMBDA_FUNCTION_NAME", "app");
    try {
      expect(() =>
        createJazz({
          app,
          permissions,
          store: vercel({ namespace: legacyNamespace, region: "iad1", local: memory() }),
        }),
      ).toThrow(/vercel\(\) requires a Vercel production or preview deployment/);
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("runs its local store outside a Vercel deployment", async () => {
    const appId = randomUUID();
    const fake = fakeVercel({ appId, url: "http://unused.invalid" });
    const before = process.env.VERCEL;
    delete process.env.VERCEL;
    cleanups.push(async () => {
      if (before !== undefined) process.env.VERCEL = before;
    });
    const jazz = await createJazz({
      app,
      permissions,
      store: vercel({ namespace: legacyNamespace, region: "iad1", sdk: fake.sdk, local: memory() }),
    }).ready();
    cleanups.push(() => jazz.close());
    await jazz.db.insert(app.notes, { text: "local" }).wait({ tier: "global" });
    expect(fake.calls).toEqual([]);
  }, 60_000);

  it("restarts an older core on this version, and never moves a newer one back", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });

    const older = fakeVercel({ ...core, appId });
    Object.assign(older.state, { exists: true, drive: true, status: "running", running: "1.0.0" });
    const upgraded = await createJazz({
      app,
      permissions,
      store: vercel({
        namespace: legacyNamespace,
        region: "iad1",
        sdk: older.sdk,
        backendCredential: core.credential,
      }),
    }).ready();
    cleanups.push(() => upgraded.close());
    expect(older.calls.filter((call) => call.startsWith("start"))).toHaveLength(1);
    expect(older.state.running).not.toBe("1.0.0");
    // The next deployment is another process: this one's Jazz closes first.
    await upgraded.close();

    const newer = fakeVercel({ ...core, appId });
    Object.assign(newer.state, { exists: true, drive: true, status: "running", running: "99.0.0" });
    const old = await createJazz({
      app,
      permissions,
      store: vercel({
        namespace: legacyNamespace,
        region: "iad1",
        sdk: newer.sdk,
        backendCredential: core.credential,
      }),
    }).ready();
    cleanups.push(() => old.close());
    expect(newer.calls.some((call) => call.startsWith("start"))).toBe(false);
    expect(newer.state.running).toBe("99.0.0");
  }, 60_000);

  it("reports a preview without a Git branch when it opens, not when createJazz is called", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    onVercel({
      VERCEL: "1",
      VERCEL_ENV: "preview",
      VERCEL_PROJECT_ID: "prj_test",
      VERCEL_GIT_COMMIT_REF: "",
    });
    // A module may call createJazz at its top level (and `next build` loads it).
    const jazz = createJazz({
      app,
      permissions,
      store: vercel({
        namespace: legacyNamespace,
        region: "iad1",
        sdk: fake.sdk,
        backendCredential: core.credential,
      }),
    });
    cleanups.push(() => jazz.close());
    await expect(jazz.ready()).rejects.toThrow(/no Git branch/);
    expect(fake.calls).toEqual([]);
  });

  it("uses the cloud core only in production and preview deployments", () => {
    expect(onVercelDeployment({ VERCEL: "1", VERCEL_ENV: "production" })).toBe(true);
    expect(onVercelDeployment({ VERCEL: "1", VERCEL_ENV: "preview" })).toBe(true);
    expect(onVercelDeployment({ VERCEL: "1", VERCEL_ENV: "development" })).toBe(false);
    expect(onVercelDeployment({})).toBe(false);
    // The serverless host checks (no sqlite, sockets via upgradeWebSocket)
    // exempt only `vercel dev`.
    expect(onVercelHost({ VERCEL: "1" })).toBe(true);
    expect(onVercelHost({ VERCEL: "1", VERCEL_ENV: "preview" })).toBe(true);
    expect(onVercelHost({ VERCEL: "1", VERCEL_ENV: "development" })).toBe(false);
    expect(onVercelHost({})).toBe(false);
  });

  it("orders versions as semver, prereleases before their release", () => {
    expect(newerVersion("2.0.0-alpha.60", "2.0.0-alpha.59")).toBe(true);
    expect(newerVersion("2.0.0-alpha.100", "2.0.0-alpha.99")).toBe(true);
    expect(newerVersion("2.0.0", "2.0.0-alpha.60")).toBe(true);
    expect(newerVersion("2.0.0-alpha.60", "2.0.0")).toBe(false);
    expect(newerVersion("2.0.0", "2.0.0")).toBe(false);
  });

  it("generates the app id on the Drive", () => {
    const trust = { issuer: "https://oidc.vercel.com/t", audience: AUDIENCE, subject: SUBJECT };
    const script = startScript("2.0.0-alpha.60", "https://example.invalid/t.tgz", trust);
    expect(script).toContain("app_id=$(cat /proc/sys/kernel/random/uuid)");
    expect(script).toContain("exec node -e 'const [addon, appId, options]");
    expect(script).toContain('"$addon" "$app_id"');
    expect(script).toContain('"port":1625');
    expect(script).not.toContain("SECRET");
  });

  it("installs the core addon for the running version once, and the app id once", () => {
    const trust = { issuer: "https://oidc.vercel.com/t", audience: AUDIENCE, subject: SUBJECT };
    const script = startScript(
      "2.0.0-alpha.60",
      "https://registry.npmjs.org/@garden-co/jazz-napi-linux-x64-gnu/-/jazz-napi-linux-x64-gnu-2.0.0-alpha.60.tgz",
      trust,
    );
    expect(script).toContain("version='2.0.0-alpha.60'");
    expect(script).toContain("addon=$data/bin/jazz-napi-$version.node");
    expect(script).toContain(`if [ ! -s "$data/app-id" ]`);
  });

  it("lets a custom environment reach its own core", () => {
    const staging = workloadOf(
      oidcToken("https://oidc.vercel.com/t", "owner:t:project:app:environment:staging"),
    );
    expect(() =>
      refuseOtherEnvironment(staging, { VERCEL_ENV: "preview", VERCEL_TARGET_ENV: "staging" }),
    ).not.toThrow();
    expect(() => refuseOtherEnvironment(staging, { VERCEL_ENV: "preview" })).toThrow(
      /OIDC token is for/,
    );
  });

  it("reads the trusted workload from the function's token", () => {
    expect(workloadOf(oidcToken("https://oidc.vercel.com/garden-co"))).toEqual({
      issuer: "https://oidc.vercel.com/garden-co",
      audience: AUDIENCE,
      subject: SUBJECT,
    });
    expect(() => workloadOf("not-a-token")).toThrow(/not an OIDC token/);
  });
});
