import { createHmac, randomUUID } from "node:crypto";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../index.js";
import { startLocalJazzServer } from "../../dev/dev-server.js";
import { createJazz, vercel, vercelBlob } from "../index.js";
import {
  defaultCoreName,
  newerVersion,
  openVercelCore,
  onVercelDeployment,
  onVercelHost,
  refuseOtherEnvironment,
  startScript,
  workloadOf,
  type SandboxHandle,
  type SandboxSdk,
} from "./vercel-store.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});

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
  };
  const files = new Map<string, Buffer>([["/data/app-id", Buffer.from(`${core.appId}\n`)]]);
  const handle = (): SandboxHandle => ({
    get status() {
      return state.status;
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
        calls.push(`create ${String(params.name)}`);
        state.exists = true;
        state.status = "running";
        return handle();
      },
    },
    Drive: {
      async getOrCreate(params) {
        calls.push(`drive ${String(params.name)}`);
        state.drive = true;
        return {};
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
      store: vercel({ sdk: fake.sdk, backendCredential: core.credential }),
    }).ready();
    cleanups.push(() => jazz.close());
    const name = defaultCoreName("prj_test", {
      VERCEL_ENV: "preview",
      VERCEL_GIT_COMMIT_REF: "main",
    });
    expect(fake.calls.slice(0, 2)).toEqual([`drive ${name}-data`, `create ${name}`]);
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
      store: vercel({ sdk: fake.sdk, backendCredential: core.credential }),
    }).ready();
    cleanups.push(() => again.close());
    const seen = await again.db.all(app.notes.where({ id: row.id }));
    expect(seen.map((note) => note.text)).toEqual(["on vercel"]);
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
        store: vercel({ sdk: fresh.sdk, backendCredential: preview }),
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
      store: vercel({ sdk: fake.sdk, backendCredential: core.credential }),
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
        store: vercel({ sdk: fake.sdk, backendCredential: preview }),
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
        store: vercel({ sdk: fake.sdk, backendCredential: preview }),
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
      createJazz({ app, permissions, store: vercel({ sdk: fake.sdk }) }).ready(),
    ).rejects.toThrow(/preview deployment's OIDC token is for .*environment:production/);
    expect(fake.calls).toEqual([]);
  }, 60_000);

  it("stops the Sandbox of a core that can't start, and holds off retrying", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId });
    const opened = await openVercelCore({
      options: { sdk: fake.sdk, backendCredential: core.credential, name: "jazz-test" },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production" },
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
      /core jazz-test could not start: could not download the core for 1\.2\.3 .*HTTP 404/,
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
          name: "jazz-test",
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
        env: { VERCEL_ENV: "production" },
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
      options: { sdk: fake.sdk, backendCredential: core.credential, name: "jazz-test" },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production" },
    });
    cleanups.push(() => opened.close());
    expect(fake.calls).not.toContain("stop");
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
      options: { sdk: fake.sdk, backendCredential: core.credential, name: "jazz-test" },
      openRemote: ({ url }) =>
        ({ serverUrl: url, handle: async () => ({}), close: async () => {} }) as never,
      version: "1.2.3",
      env: { VERCEL_ENV: "production" },
    });
    cleanups.push(() => opened.close());
    expect(fake.state.status).toBe("running");
    expect(fake.calls).not.toContain("stop");
    expect(fake.calls.filter((call) => call.startsWith("start ")).length).toBeGreaterThanOrEqual(3);
  }, 60_000);

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
      store: vercel({ sdk: fake.sdk, backendCredential: core.credential }),
    }).ready();
    cleanups.push(() => jazz.close());
    expect(fake.calls).toContain("resume jazz-prj-test-production");
    expect(fake.calls.some((call) => call.startsWith("create"))).toBe(false);
  }, 60_000);

  it("runs its local store outside a Vercel deployment", async () => {
    const appId = randomUUID();
    const fake = fakeVercel({ appId, url: "http://unused.invalid" });
    const before = process.env.VERCEL;
    delete process.env.VERCEL;
    cleanups.push(async () => {
      if (before !== undefined) process.env.VERCEL = before;
    });
    const { memory } = await import("../index.js");
    const jazz = await createJazz({
      app,
      permissions,
      store: vercel({ sdk: fake.sdk, local: memory() }),
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
      store: vercel({ sdk: older.sdk, backendCredential: core.credential }),
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
      store: vercel({ sdk: newer.sdk, backendCredential: core.credential }),
    }).ready();
    cleanups.push(() => old.close());
    expect(newer.calls.some((call) => call.startsWith("start"))).toBe(false);
    expect(newer.state.running).toBe("99.0.0");
  }, 60_000);

  it("gives each preview branch its own core", () => {
    const main = defaultCoreName("prj_1", { VERCEL_ENV: "production" });
    const a = defaultCoreName("prj_1", { VERCEL_ENV: "preview", VERCEL_GIT_COMMIT_REF: "feat/A" });
    const b = defaultCoreName("prj_1", { VERCEL_ENV: "preview", VERCEL_GIT_COMMIT_REF: "feat-a" });
    expect(main).toBe("jazz-prj-1-production");
    expect(a).toMatch(/^jazz-prj-1-preview-feat-a-[0-9a-f]{8}$/);
    expect(new Set([main, a, b]).size).toBe(3);
  });

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
      store: vercel({ sdk: fake.sdk, backendCredential: core.credential }),
    });
    cleanups.push(() => jazz.close());
    await expect(jazz.ready()).rejects.toThrow(/no Git branch/);
    expect(fake.calls).toEqual([]);
  });

  it("asks for a name when a preview deployment has no Git branch", () => {
    // Deploys through the API, or from the CLI outside a Git checkout, run
    // with an empty branch; they must not all share one core.
    for (const ref of ["", undefined]) {
      expect(() =>
        defaultCoreName("prj_1", { VERCEL_ENV: "preview", VERCEL_GIT_COMMIT_REF: ref }),
      ).toThrow(/no Git branch.*vercel\(\{ name \}\)/);
    }
    expect(defaultCoreName("prj_1", { VERCEL_ENV: "production", VERCEL_GIT_COMMIT_REF: "" })).toBe(
      "jazz-prj-1-production",
    );
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
