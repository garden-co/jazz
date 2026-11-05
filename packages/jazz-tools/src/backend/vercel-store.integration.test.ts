import { createHmac, randomUUID } from "node:crypto";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { createJazz, vercel } from "./index.js";
import {
  defaultCoreName,
  newerVersion,
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
  };
  const files = new Map<string, Buffer>([["/data/app-id", Buffer.from(`${core.appId}\n`)]]);
  const handle = (): SandboxHandle => ({
    get status() {
      return state.status;
    },
    domain: () => core.url,
    currentSession: () => ({
      startedAt: state.startedAt,
      timeout: state.timeout,
      extendTimeout: async (ms: number) => {
        calls.push(`extend ${ms}`);
        state.timeout += ms;
      },
    }),
    runCommand: async ({ args }) => {
      const script = files.get(/exec bash (\S+)/.exec(args.at(-1)!)![1]!)!.toString();
      const version = /^version='(.*)'$/m.exec(script)![1]!;
      // Like the real script: the first start pins the trust, others don't move it.
      const trust = /^trust='(.*)'$/m.exec(script)![1]!;
      const pinned = files.get("/data/trust")?.toString().trim();
      if (pinned && pinned !== trust) return;
      files.set("/data/trust", Buffer.from(`${trust}\n`));
      if (!state.running || newerVersion(version, state.running)) state.running = version;
      calls.push(`start ${version}`);
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
    onVercel({ VERCEL: "1", VERCEL_ENV: "preview", VERCEL_PROJECT_ID: "prj_test" });

    const jazz = await createJazz({
      app,
      permissions,
      store: vercel({ sdk: fake.sdk, backendCredential: core.credential }),
    }).ready();
    cleanups.push(() => jazz.close());
    const name = defaultCoreName("prj_test", { VERCEL_ENV: "preview" });
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
    onVercel({ VERCEL: "1", VERCEL_ENV: "preview", VERCEL_PROJECT_ID: "prj_test" });
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
    onVercel({ VERCEL: "1", VERCEL_ENV: "preview", VERCEL_PROJECT_ID: "prj_test" });
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
    const { memory } = await import("./index.js");
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

  it("refuses a Sandbox that holds another app's core", async () => {
    const appId = randomUUID();
    const core = await startTrustedCore(appId);
    const fake = fakeVercel({ ...core, appId: randomUUID() });
    fake.state.exists = true;
    fake.state.drive = true;
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });
    await expect(
      createJazz({
        app,
        permissions,
        store: vercel({ sdk: fake.sdk, backendCredential: core.credential, appId }),
      }).ready(),
    ).rejects.toThrow(/belongs to app/);
  }, 60_000);

  it("generates the app id on the Drive unless one is given", () => {
    const trust = { issuer: "https://oidc.vercel.com/t", audience: AUDIENCE, subject: SUBJECT };
    const script = startScript(undefined, "2.0.0-alpha.60", "https://example.invalid/t.tgz", trust);
    expect(script).toContain("app_id=$(cat /proc/sys/kernel/random/uuid)");
    expect(script).toContain("exec node -e 'const [addon, appId, options]");
    expect(script).toContain('"$addon" "$app_id"');
    expect(script).toContain('"port":1625');
    expect(script).not.toContain("SECRET");
  });

  it("installs the core addon for the running version once, and the app id once", () => {
    const trust = { issuer: "https://oidc.vercel.com/t", audience: AUDIENCE, subject: SUBJECT };
    const script = startScript(
      "app'1",
      "2.0.0-alpha.60",
      "https://registry.npmjs.org/@garden-co/jazz-napi-linux-x64-gnu/-/jazz-napi-linux-x64-gnu-2.0.0-alpha.60.tgz",
      trust,
    );
    expect(script).toContain("version='2.0.0-alpha.60'");
    expect(script).toContain("addon=$data/bin/jazz-napi-$version.node");
    expect(script).toContain(`if [ ! -s "$data/app-id" ]`);
    expect(script).toContain(`app_id='app'\\''1'`);
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
