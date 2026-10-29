import { createServer, type IncomingMessage, type Server } from "node:http";
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { runCloudCommand, type CloudCommandIo } from "./commands.js";
import { withCredentialsLock } from "./credentials.js";
import { getAccessToken } from "./session.js";

// The CLI runs against a real local HTTP server that plays both WorkOS (device
// authorization + token endpoints) and the dashboard's /api/v1.

interface Recorded {
  method: string;
  path: string;
  headers: IncomingMessage["headers"];
  body: string;
}

function jwt(claims: Record<string, unknown>): string {
  const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "RS256" })}.${encode(claims)}.signature`;
}

const nowSeconds = () => Math.floor(Date.now() / 1000);

class FakeCloud {
  server!: Server;
  url = "";
  requests: Recorded[] = [];
  pollResponses: Array<{ status: number; body: unknown }> = [];
  refreshStatus = 200;
  refreshCount = 0;
  /** Refresh tokens are single-use, as at WorkOS. */
  consumedRefreshTokens = new Set<string>();
  refreshDelayMs = 0;
  workosApiBaseUrl = "";
  revokedSessions = 0;
  validTokens = new Set<string>();
  teams = [{ teamId: "org_a", name: "Team A", role: "admin", isAdmin: true, appCount: 1 }];
  apiResponses = new Map<string, { status: number; body: unknown }>();

  issueAccessToken(lifetimeSeconds = 300): string {
    const token = jwt({ sub: "user_1", exp: nowSeconds() + lifetimeSeconds, n: Math.random() });
    this.validTokens.add(token);
    return token;
  }

  async start() {
    this.server = createServer(async (request, response) => {
      let body = "";
      for await (const chunk of request) body += chunk;
      const path = request.url ?? "";
      this.requests.push({ method: request.method ?? "", path, headers: request.headers, body });
      const send = (status: number, value: unknown) => {
        response.writeHead(status, { "content-type": "application/json" });
        response.end(JSON.stringify(value));
      };

      if (path === "/api/v1/cli/config") {
        return send(200, {
          apiVersion: 1,
          workos: {
            clientId: "client_x",
            apiBaseUrl: this.workosApiBaseUrl || `${this.url}/workos`,
          },
        });
      }
      if (path === "/workos/user_management/authorize/device") {
        return send(200, {
          device_code: "device-secret",
          user_code: "ABCD-EFGH",
          verification_uri: `${this.url}/activate`,
          verification_uri_complete: `${this.url}/activate?user_code=ABCD-EFGH`,
          expires_in: 300,
          interval: 5,
        });
      }
      if (path === "/workos/user_management/authenticate") {
        const form = new URLSearchParams(body);
        if (form.get("grant_type") === "refresh_token") {
          this.refreshCount += 1;
          const presented = form.get("refresh_token") ?? "";
          const reused = this.consumedRefreshTokens.has(presented);
          this.consumedRefreshTokens.add(presented);
          if (this.refreshDelayMs) await new Promise((r) => setTimeout(r, this.refreshDelayMs));
          if (this.refreshStatus !== 200 || reused)
            return send(reused ? 400 : this.refreshStatus, { error: "invalid_grant" });
          return send(200, {
            access_token: this.issueAccessToken(),
            refresh_token: `refresh-${this.refreshCount + 1}`,
            user: { id: "user_1", email: "ada@example.com" },
          });
        }
        const next = this.pollResponses.shift() ?? {
          status: 400,
          body: { error: "expired_token" },
        };
        return send(next.status, next.body);
      }
      if (path.startsWith("/api/v1/")) {
        const token = /^Bearer (.+)$/.exec(request.headers.authorization ?? "")?.[1];
        if (!token || !this.validTokens.has(token)) {
          return send(401, {
            error: "unauthenticated",
            message: "Invalid or expired access token.",
          });
        }
        const key = `${request.method} ${path}`;
        const canned = this.apiResponses.get(key);
        if (canned) return send(canned.status, canned.body);
        if (key === "GET /api/v1/me") {
          return send(200, { user: { id: "user_1", email: "ada@example.com" }, teams: this.teams });
        }
        if (key === "GET /api/v1/teams") return send(200, { teams: this.teams });
        if (key === "POST /api/v1/logout") {
          this.revokedSessions += 1;
          return send(200, { revoked: true });
        }
        return send(404, { error: "not_found", message: `No fake for ${key}` });
      }
      send(404, { error: "not_found" });
    });
    await new Promise<void>((resolve) => this.server.listen(0, "127.0.0.1", resolve));
    const address = this.server.address();
    if (!address || typeof address === "string") throw new Error("no address");
    this.url = `http://127.0.0.1:${address.port}`;
  }

  async stop() {
    await new Promise((resolve) => this.server.close(resolve));
  }
}

let cloud: FakeCloud;
let configDir: string;

function io(overrides: Partial<CloudCommandIo> = {}) {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const opened: string[] = [];
  const sleeps: number[] = [];
  const value: CloudCommandIo = {
    stdout: (text) => stdout.push(text),
    stderr: (text) => stderr.push(text),
    env: { NODE_ENV: "test", JAZZ_CONFIG_DIR: configDir, JAZZ_CLOUD_URL: cloud.url },
    openBrowser: (url) => opened.push(url),
    sleep: async (ms) => {
      sleeps.push(ms);
    },
    ...overrides,
  };
  return {
    io: value,
    stdout: () => stdout.join(""),
    stderr: () => stderr.join(""),
    json: () => JSON.parse(stdout.join("")),
    opened,
    sleeps,
  };
}

/** Hold one real HTTP response so another command can complete in between. */
function holdResponse(path: string) {
  let entered!: () => void;
  let release!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  let held = false;
  const fetchImpl: typeof fetch = async (input, init) => {
    const response = await fetch(input, init);
    if (!held && new URL(String(input)).pathname === path) {
      held = true;
      entered();
      await released;
    }
    return response;
  };
  return { started, release, fetch: fetchImpl };
}

async function storeLogin(accessToken: string, refreshToken = "refresh-1") {
  await writeFile(
    join(configDir, "credentials.json"),
    JSON.stringify({
      version: 1,
      profiles: {
        [cloud.url]: {
          cloudUrl: cloud.url,
          workos: { clientId: "client_x", apiBaseUrl: `${cloud.url}/workos` },
          accessToken,
          accessTokenExpiresAt: JSON.parse(
            Buffer.from(accessToken.split(".")[1]!, "base64url").toString(),
          ).exp,
          refreshToken,
          user: { id: "user_1", email: "ada@example.com" },
          createdAt: "2026-09-28T00:00:00.000Z",
        },
      },
    }),
    { mode: 0o600 },
  );
}

async function storedProfile() {
  const file = JSON.parse(await readFile(join(configDir, "credentials.json"), "utf8"));
  return file.profiles[cloud.url];
}

beforeEach(async () => {
  cloud = new FakeCloud();
  await cloud.start();
  configDir = await mkdtemp(join(tmpdir(), "jazz-cloud-cli-"));
});

afterEach(async () => {
  await cloud.stop();
  await rm(configDir, { recursive: true, force: true });
});

describe("jazz-tools login", () => {
  it("completes the device flow, honours slow_down, and stores a private session", async () => {
    const accessToken = cloud.issueAccessToken();
    cloud.pollResponses = [
      { status: 400, body: { error: "authorization_pending" } },
      { status: 400, body: { error: "slow_down" } },
      {
        status: 200,
        body: {
          access_token: accessToken,
          refresh_token: "refresh-1",
          user: { id: "user_1", email: "ada@example.com" },
          organization_id: "org_a",
        },
      },
    ];
    const run = io();

    expect(await runCloudCommand(["login", "--json"], run.io)).toBe(0);

    const lines = run
      .stdout()
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    expect(lines).toEqual([
      {
        event: "login_pending",
        cloudUrl: cloud.url,
        verificationUri: `${cloud.url}/activate`,
        verificationUriComplete: `${cloud.url}/activate?user_code=ABCD-EFGH`,
        userCode: "ABCD-EFGH",
        expiresIn: 300,
      },
      {
        event: "login_complete",
        cloudUrl: cloud.url,
        user: { id: "user_1", email: "ada@example.com" },
      },
    ]);
    expect(run.opened).toEqual([`${cloud.url}/activate?user_code=ABCD-EFGH`]);
    expect(run.sleeps).toEqual([5000, 5000, 10000]);
    // The device code is sent only to the token endpoint, never printed.
    expect(run.stdout()).not.toContain("device-secret");

    const profile = await storedProfile();
    expect(profile).toMatchObject({
      accessToken,
      refreshToken: "refresh-1",
      user: { email: "ada@example.com" },
    });
    if (process.platform !== "win32") {
      expect((await stat(join(configDir, "credentials.json"))).mode & 0o777).toBe(0o600);
    }
  });

  it("reports a denied login without storing anything", async () => {
    cloud.pollResponses = [{ status: 400, body: { error: "access_denied" } }];
    const run = io();
    expect(await runCloudCommand(["login", "--json", "--no-browser"], run.io)).toBe(1);
    const lines = run
      .stdout()
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    expect(lines.at(-1)).toEqual({
      error: "login_denied",
      message: "Login was denied in the browser.",
    });
    expect(run.opened).toEqual([]);
    await expect(readFile(join(configDir, "credentials.json"), "utf8")).rejects.toThrow();
  });

  it("refuses a non-HTTPS WorkOS API URL from the dashboard", async () => {
    cloud.workosApiBaseUrl = "http://workos.example";
    const run = io();
    expect(await runCloudCommand(["login", "--json", "--no-browser"], run.io)).toBe(1);
    expect(run.stdout()).toContain("WorkOS API URL must use HTTPS");
    expect(cloud.requests.some((request) => request.path.includes("authorize/device"))).toBe(false);
  });

  it("refuses non-HTTPS cloud URLs other than localhost", async () => {
    const run = io();
    expect(
      await runCloudCommand(
        ["whoami", "--json", "--cloud-url", "http://dashboard.example"],
        run.io,
      ),
    ).toBe(1);
    expect(run.json().message).toContain("must use HTTPS");
  });
});

describe("session handling", () => {
  it("asks for login when there is no session", async () => {
    const run = io();
    expect(await runCloudCommand(["whoami", "--json"], run.io)).toBe(1);
    expect(run.json()).toEqual({
      error: "not_logged_in",
      message: `Not logged in to ${cloud.url}. Run \`jazz-tools login\`.`,
    });
  });

  it("refreshes an expired access token and stores the rotated refresh token", async () => {
    await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() - 10 }));
    const run = io();
    expect(await runCloudCommand(["whoami", "--json"], run.io)).toBe(0);
    expect(run.json()).toMatchObject({ user: { email: "ada@example.com" }, teams: cloud.teams });
    expect(cloud.refreshCount).toBe(1);
    const refresh = cloud.requests.find((request) =>
      request.body.includes("grant_type=refresh_token"),
    );
    expect(new URLSearchParams(refresh!.body).get("refresh_token")).toBe("refresh-1");
    expect((await storedProfile()).refreshToken).toBe("refresh-2");
  });

  it("refreshes once and retries when the API rejects a stored token", async () => {
    await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() + 300 })); // not known to the server
    const run = io();
    expect(await runCloudCommand(["teams", "list", "--json"], run.io)).toBe(0);
    expect(cloud.refreshCount).toBe(1);
    expect(cloud.requests.filter((request) => request.path === "/api/v1/teams")).toHaveLength(2);
  });

  it("forgets a revoked session", async () => {
    await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() - 10 }));
    cloud.refreshStatus = 400;
    const run = io();
    expect(await runCloudCommand(["whoami", "--json"], run.io)).toBe(1);
    expect(run.json().error).toBe("session_expired");
    await expect(readFile(join(configDir, "credentials.json"), "utf8")).rejects.toThrow();
  });

  it("uses JAZZ_CLOUD_TOKEN instead of the stored login", async () => {
    const token = cloud.issueAccessToken();
    const run = io();
    run.io.env.JAZZ_CLOUD_TOKEN = token;
    expect(await runCloudCommand(["whoami", "--json"], run.io)).toBe(0);
    const me = cloud.requests.find((request) => request.path === "/api/v1/me");
    expect(me!.headers.authorization).toBe(`Bearer ${token}`);
  });

  it("logout ends the session on the server and removes it locally", async () => {
    await storeLogin(cloud.issueAccessToken());
    const run = io();
    expect(await runCloudCommand(["logout", "--json"], run.io)).toBe(0);
    expect(run.json()).toEqual({ loggedOut: true, sessionRevoked: true, cloudUrl: cloud.url });
    expect(cloud.revokedSessions).toBe(1);
    await expect(readFile(join(configDir, "credentials.json"), "utf8")).rejects.toThrow();
  });

  it("logout still forgets the session when the server cannot end it", async () => {
    await storeLogin(cloud.issueAccessToken());
    cloud.apiResponses.set("POST /api/v1/logout", { status: 404, body: { error: "not_found" } });
    const run = io();
    expect(await runCloudCommand(["logout", "--json"], run.io)).toBe(0);
    expect(run.json()).toEqual({ loggedOut: true, sessionRevoked: false, cloudUrl: cloud.url });
    await expect(readFile(join(configDir, "credentials.json"), "utf8")).rejects.toThrow();
  });

  it.each(["valid", "expired", "rejected", "revoked"] as const)(
    "logout of a %s token preserves a login completed while its response was pending",
    async (kind) => {
      const oldToken =
        kind === "valid"
          ? cloud.issueAccessToken()
          : jwt({ sub: "user_1", exp: nowSeconds() + (kind === "rejected" ? 300 : -10) });
      await storeLogin(oldToken);
      if (kind === "revoked") cloud.refreshStatus = 400;
      const hold = holdResponse(
        kind === "expired" || kind === "revoked"
          ? "/workos/user_management/authenticate"
          : "/api/v1/logout",
      );
      const logoutRun = io({ fetch: hold.fetch });
      const logout = runCloudCommand(["logout", "--json"], logoutRun.io);
      try {
        await hold.started;
        const newToken = cloud.issueAccessToken();
        cloud.pollResponses.push({
          status: 200,
          body: {
            access_token: newToken,
            refresh_token: "new-login-refresh",
            user: { id: "user_1", email: "ada@example.com" },
          },
        });
        const loginRun = io();
        expect(await runCloudCommand(["login", "--json", "--no-browser"], loginRun.io)).toBe(0);
        const replacement = await storedProfile();
        expect(replacement.accessToken).toBe(newToken);
        hold.release();
        expect(await logout).toBe(0);
        expect(logoutRun.json()).toEqual({
          loggedOut: true,
          sessionRevoked: kind !== "revoked",
          cloudUrl: cloud.url,
        });
        expect(await storedProfile()).toEqual(replacement);
        expect(cloud.consumedRefreshTokens).toEqual(new Set(kind === "valid" ? [] : ["refresh-1"]));
        expect(
          cloud.requests
            .filter((request) => request.path === "/api/v1/logout")
            .every((request) => request.headers.authorization !== `Bearer ${newToken}`),
        ).toBe(true);
      } finally {
        hold.release();
        await logout;
      }
    },
  );

  it("concurrent logouts retire the stored session only once", async () => {
    await storeLogin(cloud.issueAccessToken());
    const hold = holdResponse("/api/v1/logout");
    const first = runCloudCommand(["logout", "--json"], io({ fetch: hold.fetch }).io);
    try {
      await hold.started;
      const second = io();
      expect(await runCloudCommand(["logout", "--json"], second.io)).toBe(0);
      expect(second.json()).toEqual({
        loggedOut: false,
        sessionRevoked: false,
        cloudUrl: cloud.url,
      });
      expect(cloud.revokedSessions).toBe(1);
      await expect(readFile(join(configDir, "credentials.json"), "utf8")).rejects.toThrow();
    } finally {
      hold.release();
      await first;
    }
  });

  it("logout waits for an already-owned refresh and revokes its rotated token", async () => {
    await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() - 10 }));
    const hold = holdResponse("/workos/user_management/authenticate");
    const run = io();
    const refresh = getAccessToken({ cloudUrl: cloud.url, env: run.io.env, fetch: hold.fetch });
    let logout: Promise<number> | undefined;
    try {
      await hold.started;
      logout = runCloudCommand(["logout", "--json"], run.io);
      hold.release();
      const rotatedToken = await refresh;
      expect(await logout).toBe(0);
      expect(run.json()).toEqual({ loggedOut: true, sessionRevoked: true, cloudUrl: cloud.url });
      expect(cloud.refreshCount).toBe(1);
      expect(cloud.consumedRefreshTokens).toEqual(new Set(["refresh-1"]));
      expect(
        cloud.requests.find((request) => request.path === "/api/v1/logout")?.headers.authorization,
      ).toBe(`Bearer ${rotatedToken}`);
      await expect(readFile(join(configDir, "credentials.json"), "utf8")).rejects.toThrow();
    } finally {
      hold.release();
      await Promise.all([refresh, logout]);
    }
  });

  it("logout ignores JAZZ_CLOUD_TOKEN and revokes only the stored session", async () => {
    const storedToken = cloud.issueAccessToken();
    await storeLogin(storedToken);
    const run = io();
    run.io.env.JAZZ_CLOUD_TOKEN = cloud.issueAccessToken();
    expect(await runCloudCommand(["logout", "--json"], run.io)).toBe(0);
    expect(
      cloud.requests.find((request) => request.path === "/api/v1/logout")?.headers.authorization,
    ).toBe(`Bearer ${storedToken}`);
    expect(run.json()).toMatchObject({ loggedOut: true, sessionRevoked: true });
  });

  it("parallel commands with an expired token refresh once and stay logged in", async () => {
    await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() - 10 }));
    cloud.refreshDelayMs = 50;
    const runs = [io(), io(), io()];
    const codes = await Promise.all(
      runs.map((run) => runCloudCommand(["whoami", "--json"], run.io)),
    );
    expect(codes).toEqual([0, 0, 0]);
    expect(cloud.refreshCount).toBe(1);
    expect((await storedProfile()).refreshToken).toBe("refresh-2");
    await expect(stat(join(configDir, "credentials.lock"))).rejects.toThrow();
  });

  it("breaks a lock left by a dead process without waiting for it to go stale", async () => {
    await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() - 10 }));
    // A pid far above any real one: process.kill(pid, 0) reports ESRCH.
    await writeFile(join(configDir, "credentials.lock"), "2147483646 deadbeef\n");
    const started = Date.now();
    expect(await runCloudCommand(["whoami", "--json"], io().io)).toBe(0);
    expect(Date.now() - started).toBeLessThan(5_000);
    await expect(stat(join(configDir, "credentials.lock"))).rejects.toThrow();
  });

  it("waits for a live holder and gives up after the timeout", async () => {
    await writeFile(join(configDir, "credentials.lock"), `${process.pid} livelock\n`);
    const env: NodeJS.ProcessEnv = { NODE_ENV: "test", JAZZ_CONFIG_DIR: configDir };
    await expect(
      withCredentialsLock(async () => "acquired", env, { timeoutMs: 100 }),
    ).rejects.toThrow("Timed out waiting for another jazz-tools process");
    expect(await readFile(join(configDir, "credentials.lock"), "utf8")).toBe(
      `${process.pid} livelock\n`,
    );
  });

  it("does not release a lock that another process took over", async () => {
    const env: NodeJS.ProcessEnv = { NODE_ENV: "test", JAZZ_CONFIG_DIR: configDir };
    await withCredentialsLock(async () => {
      // As if our lock had been broken as stale and re-taken by someone else.
      await writeFile(join(configDir, "credentials.lock"), `${process.pid} someone-else\n`);
    }, env);
    expect(await readFile(join(configDir, "credentials.lock"), "utf8")).toBe(
      `${process.pid} someone-else\n`,
    );
  });

  it("gives up on a hanging refresh before the lock can go stale, keeping the login", async () => {
    await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() - 10 }));
    cloud.refreshDelayMs = 500;
    await expect(
      getAccessToken({
        cloudUrl: cloud.url,
        env: { NODE_ENV: "test", JAZZ_CONFIG_DIR: configDir },
        refreshTimeoutMs: 50,
      }),
    ).rejects.toMatchObject({ code: "refresh_failed" });
    expect((await storedProfile()).refreshToken).toBe("refresh-1");
    await expect(stat(join(configDir, "credentials.lock"))).rejects.toThrow();
  });

  it("logout of an already revoked session still reports logged out", async () => {
    await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() - 10 }));
    cloud.refreshStatus = 400;
    const run = io();
    expect(await runCloudCommand(["logout", "--json"], run.io)).toBe(0);
    expect(run.json()).toEqual({ loggedOut: true, sessionRevoked: false, cloudUrl: cloud.url });
  });

  it("does not echo a corrupt credentials file", async () => {
    await writeFile(join(configDir, "credentials.json"), '{"refreshToken": "tok_secret_fragment', {
      mode: 0o600,
    });
    const run = io();
    expect(await runCloudCommand(["whoami", "--json"], run.io)).toBe(1);
    expect(run.stdout()).toContain("Unreadable credentials file");
    expect(run.stdout()).not.toContain("tok_secret_fragment");
  });

  it.skipIf(process.platform === "win32")(
    "tightens an existing config directory to owner-only on write",
    async () => {
      await mkdir(configDir, { recursive: true });
      await chmod(configDir, 0o755);
      await storeLogin(jwt({ sub: "user_1", exp: nowSeconds() - 10 }));
      expect(await runCloudCommand(["whoami", "--json"], io().io)).toBe(0);
      expect((await stat(configDir)).mode & 0o777).toBe(0o700);
    },
  );
});

describe("jazz-tools apps", () => {
  beforeEach(async () => {
    await storeLogin(cloud.issueAccessToken());
  });

  it("creates an app in the only team and prints its secrets once", async () => {
    cloud.apiResponses.set("POST /api/v1/teams/org_a/apps", {
      status: 201,
      body: {
        appId: "app-1",
        teamId: "org_a",
        name: "Todo",
        region: "us-east-2",
        adminSecret: "adm",
        backendSecret: "bck",
      },
    });
    const run = io();
    expect(await runCloudCommand(["apps", "create", "Todo", "--region", "us-east-2"], run.io)).toBe(
      0,
    );
    const create = cloud.requests.find((request) => request.path === "/api/v1/teams/org_a/apps");
    expect(JSON.parse(create!.body)).toEqual({ name: "Todo", region: "us-east-2" });
    expect(run.stdout()).toContain("JAZZ_ADMIN_SECRET=adm");
    expect(run.stdout()).toContain("BACKEND_SECRET=bck");
  });

  it("requires --team when the user is in several teams", async () => {
    cloud.teams.push({
      teamId: "org_b",
      name: "Team B",
      role: "member",
      isAdmin: false,
      appCount: 0,
    });
    const run = io();
    expect(await runCloudCommand(["apps", "create", "Todo", "--json"], run.io)).toBe(2);
    expect(run.json()).toMatchObject({ error: "usage" });
    expect(run.json().message).toContain("org_b (Team B)");
    expect(cloud.requests.some((request) => request.method === "POST")).toBe(false);
  });

  it("sends only the auth fields that were given, with a key read from a file", async () => {
    const keyFile = join(configDir, "key.pem");
    await writeFile(keyFile, "-----BEGIN PUBLIC KEY-----\nabc\n-----END PUBLIC KEY-----\n");
    cloud.apiResponses.set("PATCH /api/v1/apps/app-1/auth", {
      status: 202,
      body: {
        appId: "app-1",
        desiredVersion: 5,
        auth: {
          jwksUrl: "",
          jwtIssuer: "iss",
          jwtAudience: "aud",
          jwtPublicKey: "k",
          allowLocalFirstAuth: false,
        },
      },
    });
    const run = io();
    const code = await runCloudCommand(
      [
        "apps",
        "auth",
        "set",
        "app-1",
        "--jwt-public-key-file",
        keyFile,
        "--jwt-issuer",
        "iss",
        "--jwt-audience",
        "aud",
        "--jwks-url",
        "",
        "--allow-local-first-auth",
        "false",
        "--json",
      ],
      run.io,
    );
    expect(code).toBe(0);
    const patch = cloud.requests.find((request) => request.method === "PATCH");
    expect(JSON.parse(patch!.body)).toEqual({
      jwksUrl: "",
      jwtIssuer: "iss",
      jwtAudience: "aud",
      jwtPublicKey: "-----BEGIN PUBLIC KEY-----\nabc\n-----END PUBLIC KEY-----",
      allowLocalFirstAuth: false,
    });
  });

  it("will not delete without the app name", async () => {
    const run = io();
    expect(await runCloudCommand(["apps", "delete", "app-1", "--json"], run.io)).toBe(2);
    expect(cloud.requests.some((request) => request.method === "DELETE")).toBe(false);
  });

  it("surfaces API error codes and messages", async () => {
    cloud.apiResponses.set("POST /api/v1/apps/app-1/secrets/rotate", {
      status: 403,
      body: { error: "forbidden", message: "Only team admins can manage apps." },
    });
    const run = io();
    expect(await runCloudCommand(["apps", "secrets", "rotate", "app-1", "--json"], run.io)).toBe(1);
    expect(run.json()).toEqual({
      error: "forbidden",
      message: "Only team admins can manage apps.",
    });
  });

  it("takes the app ID from JAZZ_APP_ID and waits for a rollout", async () => {
    let calls = 0;
    cloud.server.prependListener("request", (request) => {
      if (request.url === "/api/v1/apps/app-env/status") {
        calls += 1;
        cloud.apiResponses.set("GET /api/v1/apps/app-env/status", {
          status: 200,
          body: {
            ready: calls >= 2,
            status: calls >= 2 ? "healthy" : "pending",
            serverVersion: "v",
            desiredVersion: 4,
          },
        });
      }
    });
    const run = io();
    run.io.env.JAZZ_APP_ID = "app-env";
    expect(await runCloudCommand(["apps", "status", "--wait", "--json"], run.io)).toBe(0);
    expect(run.json()).toMatchObject({ ready: true, status: "healthy" });
    expect(calls).toBe(2);
  });
});
