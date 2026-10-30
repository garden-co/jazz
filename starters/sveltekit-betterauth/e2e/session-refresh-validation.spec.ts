import { expect, test } from "@playwright/test";
import { join, resolve } from "node:path";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { createServer, type ViteDevServer } from "vite";
import type { Handle, RequestEvent } from "@sveltejs/kit";
import { isAPIError } from "better-auth/api";
import type * as SessionTestAuth from "./session-test-auth";

type TestAuthModule = typeof SessionTestAuth;

type CookieRecord = {
  name: string;
  value: string;
  options: Record<string, unknown>;
};

type RequestEventState = {
  event: RequestEvent;
  values: Map<string, string>;
  setCookies: CookieRecord[];
};

type RequestEventOptions = {
  method?: string;
  headers?: HeadersInit;
  body?: BodyInit;
};

type SeededSession = {
  token: string;
  cookieValues: Map<string, string>;
  sessionTokenCookieName: string;
  sessionDataCookieName: string;
};

const APP_ORIGIN = "http://localhost:5173";
const STARTER_ROOT = resolve(import.meta.dirname, "..");
const HOOKS_MODULE = resolve(STARTER_ROOT, "src/hooks.server.ts");
const TEST_AUTH_MODULE = resolve(STARTER_ROOT, "e2e/session-test-auth.ts");
const TEST_ENVIRONMENT_MODULE = "\0sveltekit-session-test-environment";

let viteServer: ViteDevServer | undefined;
let testAuth: TestAuthModule | undefined;
let handle: Handle | undefined;
let viteCacheDir: string | undefined;

function cookieHeader(values: Map<string, string>): string {
  return [...values].map(([name, value]) => `${name}=${encodeURIComponent(value)}`).join("; ");
}

function createRequestEvent(
  pathname: string,
  initialValues = new Map<string, string>(),
  options: RequestEventOptions = {},
): RequestEventState {
  const values = new Map(initialValues);
  const setCookies: CookieRecord[] = [];
  const headers = new Headers(options.headers);
  const requestCookies = cookieHeader(values);
  if (requestCookies) headers.set("cookie", requestCookies);
  const request = new Request(`${APP_ORIGIN}${pathname}`, {
    method: options.method ?? "GET",
    headers,
    body: options.body,
  });
  const cookies = {
    get(name: string) {
      return values.get(name);
    },
    getAll() {
      return [...values].map(([name, value]) => ({ name, value }));
    },
    set(name: string, value: string, cookieOptions: Record<string, unknown>) {
      values.set(name, value);
      setCookies.push({ name, value, options: cookieOptions });
    },
    delete(name: string, cookieOptions: Record<string, unknown>) {
      values.delete(name);
      setCookies.push({ name, value: "", options: cookieOptions });
    },
    serialize(name: string, value: string) {
      return `${name}=${encodeURIComponent(value)}`;
    },
  };

  return {
    event: {
      request,
      url: new URL(request.url),
      cookies,
    } as unknown as RequestEvent,
    values,
    setCookies,
  };
}

function findCookie(values: Map<string, string>, cookieName: string): string {
  const value = values.get(cookieName);
  if (!value) throw new Error(`Better Auth did not issue ${cookieName}`);
  return value;
}

function findExpiry(setCookies: CookieRecord[], cookieName: string): CookieRecord | undefined {
  return [...setCookies]
    .reverse()
    .find(
      (cookie) => cookie.name === cookieName && cookie.value === "" && cookie.options.maxAge === 0,
    );
}

async function captureRejection(promise: Promise<unknown>): Promise<unknown> {
  const rejection = await promise.then(
    () => undefined,
    (error: unknown) => error,
  );
  if (rejection === undefined) throw new Error("Expected the SvelteKit handle to reject");
  return rejection;
}

async function seedSession(): Promise<SeededSession> {
  if (!testAuth) throw new Error("Test auth fixture has not been loaded");
  const signup = createRequestEvent("/api/auth/sign-up/email", new Map(), {
    method: "POST",
  });
  testAuth.setTestRequestEvent(signup.event);
  const signupResponse = await testAuth.auth.api.signUpEmail({
    body: {
      name: "Session Refresh User",
      email: `session-refresh-${Date.now()}@example.com`,
      password: "testpassword",
    },
  });
  if (!signupResponse.token) throw new Error("Better Auth did not create a session");

  const cookieNames = await testAuth.getSessionCookieNames();
  findCookie(signup.values, cookieNames.sessionToken);
  await testAuth.makeSessionEligibleForRefresh(signupResponse.token);
  return {
    token: signupResponse.token,
    cookieValues: signup.values,
    sessionTokenCookieName: cookieNames.sessionToken,
    sessionDataCookieName: cookieNames.sessionData,
  };
}

async function invokeHandle(event: RequestEvent): Promise<Response> {
  if (!handle) throw new Error("SvelteKit handle has not been loaded");
  return handle({
    event,
    resolve: async () => new Response("resolved"),
  });
}

test.beforeEach(async () => {
  viteCacheDir = await mkdtemp(join(tmpdir(), "sveltekit-session-vite-"));
  viteServer = await createServer({
    root: STARTER_ROOT,
    cacheDir: viteCacheDir,
    configFile: false,
    plugins: [
      {
        name: "sveltekit-session-test-resolver",
        resolveId(source) {
          if (source === "$lib/auth") return TEST_AUTH_MODULE;
          if (source === "$app/environment") return TEST_ENVIRONMENT_MODULE;
          return undefined;
        },
        load(id) {
          if (id === TEST_ENVIRONMENT_MODULE) return "export const building = false;";
          return undefined;
        },
      },
    ],
    server: {
      middlewareMode: true,
    },
  });

  const hooksModule = await viteServer.ssrLoadModule(HOOKS_MODULE);
  testAuth = await viteServer.ssrLoadModule(TEST_AUTH_MODULE);
  handle = hooksModule.handle as Handle;
});

test.afterEach(async () => {
  testAuth?.clearTestRequestEvent();
  testAuth?.clearSessionUpdateGate();
  handle = undefined;
  testAuth = undefined;
  await viteServer?.close();
  viteServer = undefined;
  const cacheDir = viteCacheDir;
  viteCacheDir = undefined;
  if (cacheDir) await rm(cacheDir, { recursive: true, force: true });
});

test("redirects after a deleted session loses a real rolling renewal", async () => {
  if (!testAuth) throw new Error("Test auth fixture has not been loaded");
  const seeded = await seedSession();
  const dashboard = createRequestEvent("/dashboard", seeded.cookieValues);
  testAuth.setTestRequestEvent(dashboard.event);
  const updateGate = testAuth.pauseSessionUpdate(seeded.token);
  const handlePromise = invokeHandle(dashboard.event);

  await updateGate.reached;
  await testAuth.deleteSessionViaAdapter(seeded.token);
  updateGate.release();

  const rejection = await captureRejection(handlePromise);
  expect(rejection).toMatchObject({ status: 303, location: "/" });
  expect(findExpiry(dashboard.setCookies, seeded.sessionTokenCookieName)).toBeDefined();
  expect(findExpiry(dashboard.setCookies, seeded.sessionDataCookieName)).toBeDefined();
});

test("propagates an operational renewal error instead of redirecting", async () => {
  if (!testAuth) throw new Error("Test auth fixture has not been loaded");
  const seeded = await seedSession();
  const dashboard = createRequestEvent("/dashboard", seeded.cookieValues);
  testAuth.setTestRequestEvent(dashboard.event);
  const updateFailure = testAuth.failSessionUpdate(seeded.token);
  const handlePromise = invokeHandle(dashboard.event);

  await updateFailure.reached;
  const rejection = await captureRejection(handlePromise);
  expect(isAPIError(rejection)).toBe(true);
  expect(rejection).toMatchObject({
    status: "INTERNAL_SERVER_ERROR",
    statusCode: 500,
    body: { code: "FAILED_TO_GET_SESSION" },
  });
  expect(rejection).not.toMatchObject({ status: 303, location: "/" });
});

test("keeps Better Auth session-required endpoints at HTTP 401", async () => {
  if (!testAuth) throw new Error("Test auth fixture has not been loaded");
  const authRequest = createRequestEvent("/api/auth/update-user", new Map(), {
    method: "POST",
    headers: {
      "content-type": "application/json",
      origin: APP_ORIGIN,
    },
    body: JSON.stringify({ name: "Unauthenticated" }),
  });
  testAuth.setTestRequestEvent(authRequest.event);

  const response = await invokeHandle(authRequest.event);
  expect(response.status).toBe(401);
});
