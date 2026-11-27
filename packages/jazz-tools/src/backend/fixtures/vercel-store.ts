/**
 * Shared setup for the vercel() store integration tests, split across files
 * (vercel-store*.integration.test.ts) so their slow wake and retry cases run
 * in parallel workers instead of one after another.
 */
import { createHmac } from "node:crypto";
import { createServer } from "node:http";
import { type AddressInfo } from "node:net";
import { afterEach } from "vitest";
import { testApp } from "../../testing/index.js";
import { startLocalJazzServer } from "../../dev/dev-server.js";
import { newerVersion, perBranch, type SandboxHandle, type SandboxSdk } from "../vercel-store.js";

export const app = testApp({ tables: { notes: { columns: { text: "string" } } } });
// Updates and deletes are denied.
export const permissions = { format: 1, tables: { notes: { read: "true", insert: "true" } } };

// The cores earlier versions created by default, production's and one per
// preview branch (see vercel-namespace.test.ts).
export const legacyNamespace = ({
  environment,
  branch,
}: {
  environment: string;
  branch?: string;
}) => (environment === "production" ? "production" : perBranch(branch));

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const cleanups: Array<() => Promise<unknown>> = [];
/** Runs the cleanups each test registered; call it once per test file. */
export function runCleanupsAfterEach() {
  afterEach(async () => {
    while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
  });
}

export const SIGNING_KEY = "synthetic-vercel-oidc-signing-key-0123456789";
export const AUDIENCE = "https://vercel.com/garden-co";
export const SUBJECT = "owner:garden-co:project:app:environment:production";

/** A stand-in for Vercel's OIDC issuer: discovery plus one HS256 key. */
export async function startIssuer(): Promise<string> {
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
export function oidcToken(issuer: string, subject = SUBJECT): string {
  const part = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const exp = Math.floor(Date.now() / 1000) + 600;
  const unsigned = `${part({ alg: "HS256", kid: "v1", typ: "JWT" })}.${part({ iss: issuer, aud: AUDIENCE, sub: subject, exp })}`;
  const signature = createHmac("sha256", SIGNING_KEY).update(unsigned).digest("base64url");
  return `${unsigned}.${signature}`;
}

/** The core as the start script runs it: trusting only the function's workload. */
export async function startTrustedCore(appId: string) {
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
export function fakeVercel(core: { appId: string; url: string }) {
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
    /** The next `failStops` stops fail as Vercel's API does when it refuses one. */
    failStops: 0,
    /** Called when the store stops the Sandbox's session. */
    onStop: undefined as (() => void) | undefined,
    /** The session (its start) in which a start last brought the core up. */
    launchedIn: undefined as Date | undefined,
    /**
     * What the Sandbox's core state check says (see `coreStateScript`);
     * by default `listening` once a start brought the core up, else `down`.
     */
    coreState: undefined as (() => string | Promise<string>) | undefined,
  };
  /** The sessions (by start time) whose recycle a waiter claimed. */
  const recycleClaims: number[] = [];
  /** A command the store runs in the Sandbox and waits for, by its `$0`. */
  const inSandbox = async (args: string[]) => {
    if (state.status !== "running") throw new Error("Status code 410 is not ok: sandbox stopped");
    if (args[2] === "jazz-core-state") {
      const said = state.coreState
        ? await state.coreState()
        : state.launchedIn === state.startedAt && !state.failing
          ? "listening 1"
          : "down";
      return { exitCode: 0, stdout: async () => `${said}\n` };
    }
    if (args[2] === "jazz-claim-recycle") {
      const session = Number(args[3]);
      const claimed = !recycleClaims.includes(session);
      if (claimed) recycleClaims.push(session);
      return { exitCode: claimed ? 0 : 1, stdout: async () => "" };
    }
    if (args[2] === "jazz-release-recycle") {
      const at = recycleClaims.indexOf(Number(args[3]));
      if (at >= 0) recycleClaims.splice(at, 1);
      calls.push("release recycle");
      return { exitCode: 0, stdout: async () => "" };
    }
    throw new Error(`unexpected command ${args.join(" ")}`);
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
      if (args[0] === "-c") return inSandbox(args);
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
      state.launchedIn = state.startedAt;
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
      if (state.failStops > 0) {
        state.failStops--;
        calls.push("stop failed");
        throw Object.assign(new Error("Status code 500 is not ok"), { response: { status: 500 } });
      }
      calls.push("stop");
      state.status = "stopped";
      state.onStop?.();
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
          // A fresh session: resuming starts one.
          if (state.status !== "running") state.startedAt = new Date();
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
        state.startedAt = new Date();
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
  return { sdk, calls, state, files, recycleClaims };
}

export function onVercel(env: Record<string, string>) {
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
