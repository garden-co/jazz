import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { createJazz, vercel } from "./index.js";
import { startScript, type SandboxHandle, type SandboxSdk } from "./vercel-store.js";

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
  const state = { exists: false, status: "stopped", drive: false };
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
      startedAt: new Date(),
      timeout: 300_000,
      extendTimeout: async () => void calls.push("extend"),
    }),
    runCommand: async ({ args }) => void calls.push(`run ${args.at(-1)}`),
    writeFiles: async (written) => {
      for (const file of written) files.set(file.path, file.content);
    },
    readFileToBuffer: async ({ path }) => files.get(path) ?? null,
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
    const name = "jazz-prj_test-preview";
    expect(fake.calls.slice(0, 3)).toEqual([
      `drive ${name}-data`,
      `create ${name}`,
      "run exec flock -n /tmp/jazz.lock bash /data/start.sh >>/data/server.log 2>&1",
    ]);
    expect(fake.files.get("/data/start.sh")?.toString()).toContain(`app_id='${appId}'`);

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
    expect(fake.calls).toContain("resume jazz-prj_test-production");
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
    expect(script).toContain("bin=/data/bin/jazz-tools-2.0.0-alpha.60");
    expect(script).toContain("if [ ! -f /data/secrets.env ]");
    expect(script).toContain(`app_id='app'\\''1'`);
  });
});
