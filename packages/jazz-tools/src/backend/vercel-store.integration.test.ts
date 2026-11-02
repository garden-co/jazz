import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { createJazz, vercel } from "./index.js";
import {
  defaultCoreName,
  newerVersion,
  onVercelDeployment,
  startScript,
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

/**
 * `@vercel/sandbox` standing in for Vercel: one Sandbox whose port routes to a
 * local jazz-tools server, and whose Drive holds that server's secrets.
 */
function fakeVercel(core: {
  appId: string;
  url: string;
  adminSecret: string;
  backendSecret: string;
}) {
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
  const files = new Map<string, Buffer>([
    [
      "/data/secrets.env",
      Buffer.from(
        `JAZZ_APP_ID=${core.appId}\nJAZZ_ADMIN_SECRET=${core.adminSecret}\nJAZZ_BACKEND_SECRET=${core.backendSecret}\n`,
      ),
    ],
  ]);
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
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const fake = fakeVercel({ ...core, appId });
    onVercel({ VERCEL: "1", VERCEL_ENV: "preview", VERCEL_PROJECT_ID: "prj_test" });

    const jazz = await createJazz({ appId, app, permissions, store: vercel({ sdk: fake.sdk }) });
    cleanups.push(() => jazz.close());
    const name = defaultCoreName("prj_test", { VERCEL_ENV: "preview" });
    expect(fake.calls.slice(0, 2)).toEqual([`drive ${name}-data`, `create ${name}`]);
    expect(fake.calls[2]).toMatch(/^start /);
    const scripts = [...fake.files.keys()].filter((path) => path.startsWith("/tmp/jazz-start-"));
    expect(fake.files.get(scripts[0]!)?.toString()).toContain(`app_id='${appId}'`);

    // The backend's own database reaches the core in the Sandbox.
    const row = await jazz.db.insert(app.notes, { text: "on vercel" }).wait({ tier: "global" });
    const again = await createJazz({ appId, app, permissions, store: vercel({ sdk: fake.sdk }) });
    cleanups.push(() => again.close());
    const seen = await again.db.all(app.notes.where({ id: row.id }));
    expect(seen.map((note) => note.text)).toEqual(["on vercel"]);
  }, 60_000);

  it("resumes a stopped Sandbox instead of creating another", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const fake = fakeVercel({ ...core, appId });
    fake.state.exists = true;
    fake.state.drive = true;
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });

    const jazz = await createJazz({ appId, app, permissions, store: vercel({ sdk: fake.sdk }) });
    cleanups.push(() => jazz.close());
    expect(fake.calls).toContain("resume jazz-prj-test-production");
    expect(fake.calls.some((call) => call.startsWith("create"))).toBe(false);
  }, 60_000);

  it("runs its local store outside a Vercel deployment", async () => {
    const appId = randomUUID();
    const fake = fakeVercel({
      appId,
      url: "http://unused.invalid",
      adminSecret: "a",
      backendSecret: "b",
    });
    const before = process.env.VERCEL;
    delete process.env.VERCEL;
    cleanups.push(async () => {
      if (before !== undefined) process.env.VERCEL = before;
    });
    const { memory } = await import("./index.js");
    const jazz = await createJazz({
      appId,
      app,
      permissions,
      store: vercel({ sdk: fake.sdk, local: memory() }),
    });
    cleanups.push(() => jazz.close());
    await jazz.db.insert(app.notes, { text: "local" }).wait({ tier: "global" });
    expect(fake.calls).toEqual([]);
  }, 60_000);

  it("restarts an older core on this version, and never moves a newer one back", async () => {
    const appId = randomUUID();
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });

    const older = fakeVercel({ ...core, appId });
    Object.assign(older.state, { exists: true, drive: true, status: "running", running: "1.0.0" });
    const upgraded = await createJazz({
      appId,
      app,
      permissions,
      store: vercel({ sdk: older.sdk }),
    });
    cleanups.push(() => upgraded.close());
    expect(older.calls.filter((call) => call.startsWith("start"))).toHaveLength(1);
    expect(older.state.running).not.toBe("1.0.0");

    const newer = fakeVercel({ ...core, appId });
    Object.assign(newer.state, { exists: true, drive: true, status: "running", running: "99.0.0" });
    const old = await createJazz({ appId, app, permissions, store: vercel({ sdk: newer.sdk }) });
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
    const core = await startLocalJazzServer({ appId, inMemory: true });
    cleanups.push(() => core.stop());
    const fake = fakeVercel({ ...core, appId: randomUUID() });
    fake.state.exists = true;
    fake.state.drive = true;
    onVercel({ VERCEL: "1", VERCEL_ENV: "production", VERCEL_PROJECT_ID: "prj_test" });
    await expect(
      createJazz({ appId, app, permissions, store: vercel({ sdk: fake.sdk }) }),
    ).rejects.toThrow(/belongs to app/);
  }, 60_000);

  it("generates the app id on the Drive unless one is given", () => {
    const script = startScript(undefined, "2.0.0-alpha.60", "https://example.invalid/t.tgz");
    expect(script).toContain("app_id=$(cat /proc/sys/kernel/random/uuid)");
    expect(script).toContain(`server "$JAZZ_APP_ID" --port 1625`);
  });

  it("installs the server binary for the running version once, and generates secrets once", () => {
    const script = startScript(
      "app'1",
      "2.0.0-alpha.60",
      "https://registry.npmjs.org/jazz-tools/-/jazz-tools-2.0.0-alpha.60.tgz",
    );
    expect(script).toContain("version='2.0.0-alpha.60'");
    expect(script).toContain("bin=$data/bin/jazz-tools-$version");
    expect(script).toContain(`if [ ! -f "$data/secrets.env" ]`);
    expect(script).toContain(`app_id='app'\\''1'`);
  });
});
