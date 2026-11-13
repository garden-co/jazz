import { createHmac, randomUUID } from "node:crypto";
import {
  createServer,
  request as httpRequest,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import { Readable } from "node:stream";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";
import { generateSecret, signToken } from "../local-first-auth/index.js";
import { acceptDeviceAccounts, createJazz, createJazzClient, vercel, type Jazz } from "./index.js";
import type { SandboxHandle, SandboxSdk } from "./vercel-store.js";

// Adopter finding E4 (alpha.61): on Vercel without `ws`, local-first guests
// (`initial: "device"`) never synced over the HTTP stream, and their sign-up
// then failed to link with a `link_refused` that blamed the user. The
// vercel() store's core ran with serveCore's default, which admits no
// local-first identity. These tests run the core with the options the
// Sandbox start script gives it, behind two function instances that share it.
vi.mock("ws", () => {
  throw Object.assign(new Error("Cannot find package 'ws' imported from jazz-tools"), {
    code: "ERR_MODULE_NOT_FOUND",
  });
});

const app = s.defineApp(
  { todos: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user });
  policy.todos.allowInsert.where({ owner: session.user });
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});

/** The app's auth: the user named in a header, else a local-first guest. */
function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId
    ? { userId, expiresAt: Math.floor(Date.now() / 1000) + 3600 }
    : acceptDeviceAccounts(request);
}

const cleanups: Array<() => Promise<unknown>> = [];
const env: Record<string, string | undefined> = {};
beforeEach(() => {
  const vars = {
    VERCEL: "1",
    VERCEL_ENV: "production",
    VERCEL_PROJECT_ID: "prj_guest",
  };
  for (const [key, value] of Object.entries(vars)) {
    env[key] = process.env[key];
    process.env[key] = value;
  }
});
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  vi.restoreAllMocks();
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

function oidcToken(issuer: string): string {
  const part = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const exp = Math.floor(Date.now() / 1000) + 600;
  const unsigned = `${part({ alg: "HS256", kid: "v1", typ: "JWT" })}.${part({ iss: issuer, aud: AUDIENCE, sub: SUBJECT, exp })}`;
  const signature = createHmac("sha256", SIGNING_KEY).update(unsigned).digest("base64url");
  return `${unsigned}.${signature}`;
}

/**
 * One Sandbox, shared by every function instance. Its start runs the core
 * with the options the start script passes `serveCore`, and serveCore's
 * defaults for what it leaves out (local-first auth: off).
 */
function fakeSandbox(appId: string, issuer: string, override: Record<string, unknown> = {}) {
  const files = new Map<string, Buffer>([["/data/app-id", Buffer.from(`${appId}\n`)]]);
  const state = { exists: false, drive: false, running: undefined as string | undefined };
  let core: Promise<{ url: string }> | undefined;
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
      const options = JSON.parse(/"\$app_id" '(.*)'$/m.exec(script)![1]!) as {
        allowLocalFirstAuth?: boolean;
      };
      core ??= startLocalJazzServer({
        appId,
        inMemory: true,
        trustedWorkload: { issuer, audience: AUDIENCE, subject: SUBJECT },
        allowLocalFirstAuth: options.allowLocalFirstAuth ?? false,
        ...override,
      }).then((started) => {
        cleanups.push(() => started.stop());
        return started;
      });
      coreUrl = (await core).url;
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
  return { sdk, coreUrl: () => coreUrl };
}

/**
 * Sockets the deployment's instances open to the core, with the access each
 * states: a guest naming the app rides on `session` access, the backend's
 * own and a signed-in user's on `backend`.
 */
function watchCoreSockets() {
  const opened: { url: string; access: string }[] = [];
  const Real = globalThis.WebSocket;
  globalThis.WebSocket = class extends Real {
    constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
      const headers = options?.headers ?? {};
      if (headers["x-jazz-backend-credential"]) {
        opened.push({ url: String(url), access: headers["x-jazz-backend-access"] ?? "session" });
      }
      super(url, options as never);
    }
  } as typeof WebSocket;
  cleanups.push(async () => {
    globalThis.WebSocket = Real;
  });
  return opened;
}

/** Two function instances of one deployment: two createJazz, one Sandbox core. */
async function deployment(
  override?: Record<string, unknown>,
  auth: (request: Request) => ReturnType<typeof authenticate> | null = authenticate,
) {
  const coreSockets = watchCoreSockets();
  const appId = randomUUID();
  const issuer = await startIssuer();
  const { sdk, coreUrl } = fakeSandbox(appId, issuer, override);
  const instance = async () => {
    const jazz = await createJazz({
      app,
      permissions,
      authenticate: auth,
      store: vercel({
        namespace: ({ environment }) => environment,
        sdk,
        backendCredential: () => oidcToken(issuer),
      }),
    }).ready();
    cleanups.push(() => jazz.close());
    return jazz;
  };
  const a = await instance();
  const b = await instance();
  return { appId, instances: [a, b] as const, coreUrl: coreUrl(), coreSockets };
}

/**
 * Vercel's routing, as two instances see it: requests go to either
 * instance in turn, and an HTTP stream's sends reach the instance that
 * holds its open response.
 */
async function route(instances: readonly Jazz[]) {
  const handlers = instances.map(
    (jazz) => jazz.next() as Record<string, (r: Request) => Promise<Response>>,
  );
  const streamOwners = new Map<string, number>();
  const log: { path: string; instance: number; status: number }[] = [];
  let turn = 0;
  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    void (async () => {
      const path = req.url ?? "/";
      const send = /\/stream\/([^/?]+)/.exec(path)?.[1];
      const instance =
        send !== undefined
          ? (streamOwners.get(decodeURIComponent(send)) ?? 0)
          : turn++ % instances.length;
      const headers = new Headers();
      for (const [name, value] of Object.entries(req.headers)) {
        if (typeof value === "string") headers.set(name, value);
      }
      const hasBody = req.method !== "GET" && req.method !== "HEAD";
      const request = new Request(`http://${req.headers.host}${path}`, {
        method: req.method,
        headers,
        ...(hasBody ? { body: Readable.toWeb(req) as ReadableStream, duplex: "half" } : {}),
      } as RequestInit);
      const response = await handlers[instance]![req.method!]!(request);
      log.push({ path, instance, status: response.status });
      res.writeHead(response.status, Object.fromEntries(response.headers));
      if (!response.body) return res.end();
      const reader = response.body.getReader();
      let first = path.endsWith("/stream");
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (first) {
          // The stream's first record names its session.
          first = false;
          const id = /"id":"([^"]+)"/.exec(Buffer.from(value).toString("latin1"))?.[1];
          if (id) streamOwners.set(id, instance);
        }
        if (!res.write(value)) await new Promise((resolve) => res.once("drain", resolve));
      }
      res.end();
    })().catch((error: unknown) => {
      res.statusCode = 500;
      res.end(String(error));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  // A page on this origin: its client syncs with /jazz same-origin.
  const previous = (globalThis as { location?: unknown }).location;
  (globalThis as { location?: unknown }).location = {
    host: new URL(origin).host,
    href: `${origin}/`,
  };
  cleanups.push(async () => {
    (globalThis as { location?: unknown }).location = previous;
  });
  return { origin, log };
}

/** From now on, this test's requests to `origin` come from `userId`'s browser. */
function signInAs(origin: string, userId: string) {
  const realFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (new URL(request.url).origin === origin) request.headers.set("x-test-user", userId);
    return realFetch(request);
  }) as typeof fetch;
  cleanups.push(async () => {
    globalThis.fetch = realFetch;
  });
}

async function guestOf(appId: string, origin: string) {
  const session = await createJazzClient({
    appId,
    app,
    permissions,
    serverUrl: `${origin}/jazz`,
    driver: { type: "memory" },
    initial: "device",
  });
  cleanups.push(() => session.close());
  return session;
}

function within<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`${what} did not settle within ${ms} ms`)), ms),
    ),
  ]);
}

it("syncs a local-first guest over the HTTP stream through two instances, and keeps it on sign-up", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const { appId, instances, coreSockets } = await deployment();
  const { origin, log } = await route(instances);

  const session = await guestOf(appId, origin);
  const guest = session.getSnapshot().client!;
  const account = guest.session!.user!;
  const row = await within(
    guest.db
      .insert(app.todos, { title: "made as a guest", owner: account })
      .wait({ tier: "global" }),
    20_000,
    "the guest's write",
  );
  // It came over the stream, and both instances served the guest.
  expect(log.some(({ path }) => path.endsWith(`/apps/${appId}/stream`))).toBe(true);
  expect(log.some(({ path }) => path.endsWith("/ws"))).toBe(true);
  expect(new Set(log.map(({ instance }) => instance))).toEqual(new Set([0, 1]));
  expect(log.filter(({ status }) => status === 404)).toEqual([]);
  // The guest's own session reached the core, on session access.
  expect(coreSockets.some(({ access }) => access === "session")).toBe(true);
  // Each instance reads the guest's row from the shared core.
  for (const jazz of instances) {
    const seen = await jazz.db.all(app.todos.where({ id: row.id }));
    expect(seen.map((todo) => todo.title)).toEqual(["made as a guest"]);
  }

  // The guest signs up: the link keeps its account and its data.
  signInAs(origin, "alice");
  await within(session.linkDeviceAccount(), 20_000, "the link");
  const alice = session.getSnapshot().client!;
  expect(alice.session!.user!).toBe(account);
  const seen = await alice.db.all(app.todos.where({ id: row.id }));
  expect(seen.map((todo) => todo.title)).toEqual(["made as a guest"]);
}, 90_000);

it("says a core that admits no guests refuses them, not that the user has an account", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  // A core an older jazz-tools started: local-first auth off.
  const { appId, instances } = await deployment({ allowLocalFirstAuth: false });
  const { origin } = await route(instances);

  const session = await guestOf(appId, origin);
  const guest = session.getSnapshot().client!;
  // (No writes: linking first uploads them, and this core refuses that
  // upload with its own explicit "Local-first auth is not enabled" error.)

  signInAs(origin, "alice");
  const linked = session.linkDeviceAccount();
  await expect(linked).rejects.toMatchObject({
    code: "local_first_auth_disabled",
    message: expect.stringMatching(/core admits no device accounts/),
  });
  await expect(linked).rejects.not.toThrow(/already have an account/);
  // The guest is still this device's account.
  expect(session.getSnapshot().client!.session!.user!).toBe(guest.session!.user!);
  expect(
    error.mock.calls.filter((call) => /local_first_auth_disabled/.test(call.join(" "))),
  ).toHaveLength(1);
}, 90_000);

it("logs it on the server when the core refuses a guest's sync session", async () => {
  const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const { appId, instances } = await deployment({ allowLocalFirstAuth: false });
  const { origin } = await route(instances);

  const session = await guestOf(appId, origin);
  const guest = session.getSnapshot().client!;
  void guest.db
    .insert(app.todos, { title: "made as a guest", owner: guest.session!.user! })
    .wait({ tier: "global" })
    .catch(() => undefined);
  const logged = () =>
    error.mock.calls.filter((call) => /local_first_auth_disabled/.test(call.join(" ")));
  await vi.waitFor(() => expect(logged()).toHaveLength(1), { timeout: 20_000 });
}, 60_000);

it("admits no guest at the core except through the backend's own sockets", async () => {
  const { appId, coreUrl } = await deployment();
  // A guest's own token, straight to the core: refused without the
  // deployment's OIDC token, guest sign-in on or not.
  const token = signToken(generateSecret(), { audience: appId });
  const status = await new Promise<number>((resolve, reject) => {
    const req = httpRequest(`${coreUrl}/apps/${appId}/ws`, {
      headers: {
        connection: "Upgrade",
        upgrade: "websocket",
        "sec-websocket-version": "13",
        "sec-websocket-key": Buffer.from(randomUUID()).toString("base64").slice(0, 24),
        authorization: `Bearer ${token}`,
      },
    });
    req.on("response", (res) => {
      res.resume();
      resolve(res.statusCode!);
    });
    req.on("upgrade", (_res, socket) => {
      socket.destroy();
      reject(new Error("the core upgraded a socket without the workload's token"));
    });
    req.on("error", reject);
    req.end();
  });
  expect(status).toBe(401);
}, 60_000);

it("never opens a guest's session on the core when the app doesn't accept device accounts", async () => {
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const { appId, instances, coreSockets } = await deployment(undefined, (request) => {
    const userId = request.headers.get("x-test-user");
    return userId ? { userId, expiresAt: Math.floor(Date.now() / 1000) + 3600 } : null;
  });
  const { origin } = await route(instances);

  const session = await guestOf(appId, origin);
  const guest = session.getSnapshot().client!;
  void guest.db
    .insert(app.todos, { title: "made as a guest", owner: guest.session!.user! })
    .wait({ tier: "global" })
    .catch(() => undefined);
  await vi.waitFor(
    () =>
      expect(
        warn.mock.calls.filter((call) => /refused a local-first guest/.test(call.join(" "))),
      ).toHaveLength(1),
    { timeout: 20_000 },
  );
  expect(coreSockets.filter(({ access }) => access === "session")).toEqual([]);
}, 60_000);
