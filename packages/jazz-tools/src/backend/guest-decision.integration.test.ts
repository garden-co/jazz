import { createHmac, randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { createRequire } from "node:module";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { defineShippedApp } from "../testing/app-artifact.js";
import { schema as s } from "../index.js";
import {
  acceptDeviceAccounts,
  createJazz,
  createJazzClient,
  memory,
  remote,
  sqlite,
  vercel,
  type JazzStore,
} from "./index.js";
import type { SandboxHandle, SandboxSdk } from "./vercel-store.js";

// Which local-first guests sync is the app's call alone: `authenticate`
// returning `acceptDeviceAccounts(request)`. A core has no switch of its own
// that could disagree with it (#5248). Before, `serveCore` and
// `jazz-tools server` in production refused every guest unless started with
// `allowLocalFirstAuth`, so a `remote()` app that let guests in had them
// parked offline (as E4 did on `vercel()`). Each store kind runs here with the
// core options it really gets: the in-process core's, a production core's
// (`serveCore`) with only its secrets, and a `vercel()` core's from the
// Sandbox start script.

const { app, permissions } = await defineShippedApp(
  { todos: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  (app) =>
    s.definePermissions(app, ({ policy, session }) => {
      policy.todos.allowRead.where({ owner: session.user });
      policy.todos.allowInsert.where({ owner: session.user });
      policy.todos.allowUpdate.never();
      policy.todos.allowDelete.never();
    }),
);

// The Sandbox loads the addon with require(); its ES entry doesn't export serveCore.
const { serveCore } = createRequire(import.meta.url)("jazz-napi") as typeof import("jazz-napi");

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

async function tempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "jazz-guest-decision-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

async function listening(server: Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as AddressInfo).port;
}

async function freePort(): Promise<number> {
  const server = createServer();
  const port = await listening(server);
  await new Promise((resolve) => server.close(resolve));
  return port;
}

/** A production core, as `serveCore` runs one: these options and its defaults. */
async function servedCore(options: Record<string, unknown>): Promise<string> {
  const port = await freePort();
  const core = serveCore({
    ...(options as Parameters<typeof serveCore>[0]),
    port,
    dataDir: join(await tempDir(), "store"),
    shutdownTimeoutSecs: 1,
  });
  cleanups.push(async () => {
    core.stop();
    await core.closed();
  });
  // Up once it answers.
  await vi.waitFor(
    async () => {
      await fetch(`http://127.0.0.1:${port}/health`);
    },
    { timeout: 10_000 },
  );
  return `http://127.0.0.1:${port}`;
}

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
  const port = await listening(server);
  cleanups.push(() => new Promise((resolve) => server.close(resolve)));
  return `http://127.0.0.1:${port}`;
}

function oidcToken(issuer: string): string {
  const part = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const exp = Math.floor(Date.now() / 1000) + 600;
  const unsigned = `${part({ alg: "HS256", kid: "v1", typ: "JWT" })}.${part({ iss: issuer, aud: AUDIENCE, sub: SUBJECT, exp })}`;
  const signature = createHmac("sha256", SIGNING_KEY).update(unsigned).digest("base64url");
  return `${unsigned}.${signature}`;
}

/**
 * A deployed `vercel()` store whose Sandbox runs a real `serveCore` with
 * exactly the options the start script hands it (only the port and data
 * directory are this machine's).
 */
async function deployedVercel(): Promise<JazzStore> {
  vi.stubEnv("VERCEL", "1");
  vi.stubEnv("VERCEL_ENV", "production");
  vi.stubEnv("VERCEL_PROJECT_ID", "prj_guest_decision");
  const issuer = await startIssuer();
  const appId = randomUUID();
  const files = new Map<string, Buffer>([["/data/app-id", Buffer.from(`${appId}\n`)]]);
  const state = { exists: false, drive: false, running: undefined as string | undefined };
  let core: Promise<string> | undefined;
  let coreUrl = "http://127.0.0.1:9";
  const handle = (): SandboxHandle => ({
    status: "running",
    domain: () => coreUrl,
    currentSession: () => ({
      startedAt: new Date(),
      timeout: 300_000,
      extendTimeout: async () => undefined,
    }),
    runCommand: async ({ args }) => {
      const script = files.get(/exec bash (\S+)/.exec(args.at(-1)!)![1]!)!.toString();
      const version = /^version='(.*)'$/m.exec(script)![1]!;
      const options = JSON.parse(/"\$app_id" '(.*)'$/m.exec(script)![1]!) as Record<
        string,
        unknown
      >;
      core ??= servedCore({ ...options, appId });
      coreUrl = await core;
      state.running = version;
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
    stop: async () => undefined,
  });
  const sdk: SandboxSdk = {
    Sandbox: {
      async get(params) {
        if (!state.exists) throw new Error("404 sandbox not found");
        const sbx = handle();
        if (params.resume) await (params.onResume as (sbx: SandboxHandle) => Promise<void>)(sbx);
        return sbx;
      },
      async create() {
        if (!state.drive) throw new Error("drive missing");
        state.exists = true;
        return handle();
      },
    },
    Drive: {
      async getOrCreate() {
        state.drive = true;
        return {};
      },
    },
  };
  return vercel({
    sdk,
    backendCredential: () => oidcToken(issuer),
    namespace: ({ environment }) => environment,
  });
}

const stores: Array<[string, () => Promise<JazzStore>]> = [
  ["memory", async () => memory()],
  ["sqlite", async () => sqlite({ path: join(await tempDir(), "local") })],
  [
    "remote (a production core with only its secrets)",
    async () => {
      const appId = randomUUID();
      const secret = randomBytes(32).toString("base64url");
      const url = await servedCore({
        appId,
        blobs: { kind: "sqlite" },
        adminSecret: secret,
        backendSecret: randomBytes(32).toString("base64url"),
      });
      return remote({ url, appId, secret });
    },
  ],
  ["vercel (deployed)", deployedVercel],
];

/** The app's Jazz on `store`, served on its own origin, letting guests in or not. */
async function appOn(store: JazzStore, guests: boolean) {
  const jazz = await createJazz({
    app,
    permissions,
    store,
    authenticate: (request) => (guests ? acceptDeviceAccounts(request) : null),
  }).ready();
  const server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  const port = await listening(server);
  cleanups.push(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await jazz.close();
  });
  const session = await createJazzClient({
    appId: jazz.appId,
    app,
    permissions,
    serverUrl: `http://127.0.0.1:${port}${jazz.basePath}`,
    driver: { type: "memory" },
    initial: "device",
  });
  cleanups.push(() => session.close());
  return { jazz, guest: session.getSnapshot().client! };
}

describe.each(stores)("on a %s store", (_kind, store) => {
  it("syncs a guest the app accepts", async () => {
    const { jazz, guest } = await appOn(await store(), true);
    const row = await guest.db
      .insert(app.todos, { title: "made as a guest", owner: guest.session!.user! })
      .wait({ tier: "global" });
    const seen = await jazz.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["made as a guest"]);
  }, 60_000);

  it("refuses a guest the app doesn't accept", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { guest } = await appOn(await store(), false);
    void guest.db
      .insert(app.todos, { title: "made as a guest", owner: guest.session!.user! })
      .wait({ tier: "global" })
      .catch(() => undefined);
    await vi.waitFor(
      () =>
        expect(
          warn.mock.calls.some((call) => /refused a local-first guest/.test(call.join(" "))),
        ).toBe(true),
      { timeout: 20_000 },
    );
  }, 60_000);
});
