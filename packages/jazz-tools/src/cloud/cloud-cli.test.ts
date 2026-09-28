import { createServer, type IncomingMessage, type Server } from "node:http";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { runCloudCommand, type CloudCommandIo } from "./commands.js";

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
          workos: { clientId: "client_x", apiBaseUrl: `${this.url}/workos` },
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
          if (this.refreshStatus !== 200)
            return send(this.refreshStatus, { error: "invalid_grant" });
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

  it("logout removes the stored session", async () => {
    await storeLogin(cloud.issueAccessToken());
    const run = io();
    expect(await runCloudCommand(["logout", "--json"], run.io)).toBe(0);
    expect(run.json()).toEqual({ loggedOut: true, cloudUrl: cloud.url });
    await expect(readFile(join(configDir, "credentials.json"), "utf8")).rejects.toThrow();
  });
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
