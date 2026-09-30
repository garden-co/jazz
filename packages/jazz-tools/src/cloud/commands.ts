// `jazz-tools login | logout | whoami | teams | apps`: manage Jazz Cloud from
// the command line with the same permissions as the dashboard.
//
// Every command accepts `--json` and then prints exactly one JSON document on
// stdout (errors too: `{"error": <code>, "message": ...}`), except `login`,
// which prints one JSON line per step so an agent can hand the approval URL to
// a human while the command keeps waiting.

import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";

import { CloudApiError, createCloudApi, type CloudApi } from "./api.js";
import { credentialsPath, deleteProfile, saveProfile } from "./credentials.js";
import {
  CloudAuthError,
  fetchWorkosClientConfig,
  pollDeviceAuthorization,
  profileFromTokens,
  resolveCloudUrl,
  startDeviceAuthorization,
} from "./session.js";

export const CLOUD_COMMANDS = ["login", "logout", "whoami", "teams", "apps"] as const;

export function isCloudCommand(command: string): boolean {
  return (CLOUD_COMMANDS as readonly string[]).includes(command);
}

export interface CloudCommandIo {
  stdout: (text: string) => void;
  stderr: (text: string) => void;
  env: NodeJS.ProcessEnv;
  fetch?: typeof fetch;
  openBrowser?: (url: string) => void;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

class UsageError extends Error {}

const BOOLEAN_FLAGS = new Set([
  "--json",
  "--no-browser",
  "--wait",
  "--acknowledge-client-upgrade",
  "--help",
  "-h",
]);

interface ParsedArgs {
  positionals: string[];
  flags: Map<string, string | true>;
}

function parse(args: string[]): ParsedArgs {
  const positionals: string[] = [];
  const flags = new Map<string, string | true>();
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (!arg.startsWith("-")) {
      positionals.push(arg);
      continue;
    }
    const eq = arg.indexOf("=");
    const name = eq === -1 ? arg : arg.slice(0, eq);
    if (BOOLEAN_FLAGS.has(name)) {
      if (eq !== -1) throw new UsageError(`${name} does not take a value.`);
      flags.set(name, true);
      continue;
    }
    const value = eq === -1 ? args[++index] : arg.slice(eq + 1);
    if (value === undefined || (eq === -1 && value.startsWith("--"))) {
      throw new UsageError(`Missing value for ${name}.`);
    }
    flags.set(name, value);
  }
  return { positionals, flags };
}

function flag(parsed: ParsedArgs, name: string): string | undefined {
  const value = parsed.flags.get(name);
  return typeof value === "string" ? value : undefined;
}

function allowOnly(parsed: ParsedArgs, allowed: string[]): void {
  for (const name of parsed.flags.keys()) {
    if (!["--json", "--cloud-url", ...allowed].includes(name)) {
      throw new UsageError(`Unknown option ${name}.`);
    }
  }
}

function appIdArg(parsed: ParsedArgs, index: number, env: NodeJS.ProcessEnv): string {
  const appId = parsed.positionals[index] ?? env.JAZZ_APP_ID?.trim();
  if (!appId) throw new UsageError("An app ID is required (or set JAZZ_APP_ID).");
  return appId;
}

interface Team {
  teamId: string;
  name: string;
  role: string;
  isAdmin: boolean;
  appCount: number;
}

async function resolveTeam(api: CloudApi, requested: string | undefined): Promise<string> {
  if (requested) return requested;
  const { teams } = await api.request<{ teams: Team[] }>("GET", "/teams");
  if (teams.length === 1) return teams[0]!.teamId;
  if (teams.length === 0) throw new UsageError("Your account is not a member of any team.");
  throw new UsageError(
    `You are in ${teams.length} teams; pass --team <teamId>. Teams: ${teams
      .map((team) => `${team.teamId} (${team.name})`)
      .join(", ")}.`,
  );
}

function defaultOpenBrowser(url: string): void {
  const command =
    process.platform === "darwin" ? "open" : process.platform === "win32" ? "cmd" : "xdg-open";
  const args = process.platform === "win32" ? ["/c", "start", "", url] : [url];
  try {
    const child = spawn(command, args, { stdio: "ignore", detached: true });
    child.on("error", () => {});
    child.unref();
  } catch {
    // Printing the URL is the fallback.
  }
}

const HELP = `Jazz Cloud commands (sign in with your dashboard account):

  login                         Sign in through the browser (device code)
  logout                        Forget the stored session
  whoami                        Show the signed-in user and teams
  teams list                    List your teams
  apps list [--team <id>]       List apps (all teams by default)
  apps create <name> [--team <id>] [--region <region>]
                                Create an app; prints its secrets once
  apps get <appId>              Show an app
  apps claim <appId> --name <name> [--team <id>] [--admin-secret <secret>]
                                Move an unclaimed app into a team
  apps delete <appId> --confirm <app name>
  apps auth get <appId>         Show external JWT / local-first auth settings
  apps auth set <appId> [--jwks-url <url>] [--jwt-issuer <iss>]
                        [--jwt-audience <aud>] [--jwt-public-key <jwk|pem>]
                        [--jwt-public-key-file <path>]
                        [--allow-local-first-auth true|false]
                                Change auth settings (omitted fields keep their value;
                                pass an empty string to clear one)
  apps secrets rotate <appId>   Rotate the admin and backend secrets; prints them once
  apps status <appId> [--wait]  Show rollout status (--wait polls until ready)
  apps upgrade <appId>          List available stable-release upgrades
  apps upgrade <appId> --release <id> --expected-version <version>
                       --acknowledge-client-upgrade
                                Start an upgrade

Options:
  --json                        Machine-readable output
  --cloud-url <url>             Dashboard URL (or JAZZ_CLOUD_URL; default https://v2.dashboard.jazz.tools)
  --no-browser                  login: do not open a browser

Environment:
  JAZZ_CLOUD_TOKEN              Use this short-lived access token instead of the stored login
  JAZZ_CONFIG_DIR               Where the login is stored (default ~/.config/jazz)
`;

export async function runCloudCommand(args: string[], io: CloudCommandIo): Promise<number> {
  const json = args.includes("--json");
  const out = (value: unknown, human: string) =>
    io.stdout(json ? `${JSON.stringify(value)}\n` : human);
  try {
    const parsed = parse(args);
    const [command, sub] = parsed.positionals;
    if (parsed.flags.has("--help") || parsed.flags.has("-h")) {
      io.stdout(HELP);
      return 0;
    }
    const cloudUrl = resolveCloudUrl(flag(parsed, "--cloud-url"), io.env);
    const sessionOptions = { cloudUrl, env: io.env, fetch: io.fetch, now: io.now, sleep: io.sleep };
    const api = createCloudApi(sessionOptions);

    switch (command) {
      case "login":
        allowOnly(parsed, ["--no-browser"]);
        return await login(parsed, io, cloudUrl, json);

      case "logout": {
        allowOnly(parsed, []);
        const removed = await deleteProfile(cloudUrl, io.env);
        out(
          { loggedOut: removed, cloudUrl },
          removed ? `Logged out of ${cloudUrl}.\n` : `Not logged in to ${cloudUrl}.\n`,
        );
        return 0;
      }

      case "whoami": {
        allowOnly(parsed, []);
        const me = await api.request<{ user: { id: string; email: string }; teams: Team[] }>(
          "GET",
          "/me",
        );
        out(
          { cloudUrl, ...me },
          `${me.user.email} on ${cloudUrl}\n${me.teams
            .map((team) => `  ${team.teamId}  ${team.name}  (${team.role}, ${team.appCount} apps)`)
            .join("\n")}\n`,
        );
        return 0;
      }

      case "teams": {
        if (sub !== "list" && sub !== undefined)
          throw new UsageError(`Unknown command: teams ${sub}`);
        allowOnly(parsed, []);
        const { teams } = await api.request<{ teams: Team[] }>("GET", "/teams");
        out(
          { teams },
          teams.map((team) => `${team.teamId}  ${team.name}  (${team.role})\n`).join("") ||
            "No teams.\n",
        );
        return 0;
      }

      case "apps":
        return await apps(parsed, io, api, out);

      default:
        throw new UsageError(`Unknown command: ${command ?? ""}`);
    }
  } catch (error) {
    return reportError(error, io, json);
  }
}

async function login(
  parsed: ParsedArgs,
  io: CloudCommandIo,
  cloudUrl: string,
  json: boolean,
): Promise<number> {
  const workos = await fetchWorkosClientConfig(cloudUrl, io.fetch);
  const authorization = await startDeviceAuthorization(workos, io.fetch);
  if (json) {
    io.stdout(
      `${JSON.stringify({
        event: "login_pending",
        cloudUrl,
        verificationUri: authorization.verificationUri,
        verificationUriComplete: authorization.verificationUriComplete,
        userCode: authorization.userCode,
        expiresIn: authorization.expiresIn,
      })}\n`,
    );
  } else {
    io.stderr(
      `To sign in to ${cloudUrl}, open:\n\n  ${authorization.verificationUriComplete}\n\n` +
        `and confirm the code ${authorization.userCode}. Waiting...\n`,
    );
  }
  if (!parsed.flags.has("--no-browser")) {
    (io.openBrowser ?? defaultOpenBrowser)(authorization.verificationUriComplete);
  }
  const tokens = await pollDeviceAuthorization(workos, authorization, {
    fetch: io.fetch,
    now: io.now,
    sleep: io.sleep,
  });
  const profile = profileFromTokens(cloudUrl, workos, tokens);
  await saveProfile(profile, io.env);
  if (json) {
    io.stdout(`${JSON.stringify({ event: "login_complete", cloudUrl, user: profile.user })}\n`);
  } else {
    io.stdout(`Logged in to ${cloudUrl} as ${profile.user.email}.\n`);
    io.stderr(`Session stored in ${credentialsPath(io.env)}.\n`);
  }
  return 0;
}

interface AppAuth {
  jwksUrl: string;
  jwtIssuer: string;
  jwtAudience: string;
  jwtPublicKey: string;
  allowLocalFirstAuth: boolean;
}

function describeAuth(auth: AppAuth): string {
  const external = auth.jwksUrl
    ? `JWKS ${auth.jwksUrl}`
    : auth.jwtPublicKey
      ? "static public key"
      : "none";
  return (
    `  external JWT:       ${external}\n` +
    (auth.jwtIssuer ? `  issuer:             ${auth.jwtIssuer}\n` : "") +
    (auth.jwtAudience ? `  audience:           ${auth.jwtAudience}\n` : "") +
    `  local-first auth:   ${auth.allowLocalFirstAuth ? "allowed" : "disabled"}\n`
  );
}

function secretsText(result: {
  appId: string;
  adminSecret: string;
  backendSecret: string;
}): string {
  return (
    `  JAZZ_APP_ID=${result.appId}\n` +
    `  JAZZ_ADMIN_SECRET=${result.adminSecret}\n` +
    `  BACKEND_SECRET=${result.backendSecret}\n` +
    "These secrets are shown once. Store them in your deployment's secret manager.\n"
  );
}

async function apps(
  parsed: ParsedArgs,
  io: CloudCommandIo,
  api: CloudApi,
  out: (value: unknown, human: string) => void,
): Promise<number> {
  const sub = parsed.positionals[1];
  const encode = encodeURIComponent;

  switch (sub) {
    case "list": {
      allowOnly(parsed, ["--team"]);
      const requested = flag(parsed, "--team");
      const teamIds = requested
        ? [requested]
        : (await api.request<{ teams: Team[] }>("GET", "/teams")).teams.map((team) => team.teamId);
      const all: Array<{ appId: string; teamId: string; name: string }> = [];
      for (const teamId of teamIds) {
        const { apps } = await api.request<{ apps: typeof all }>(
          "GET",
          `/teams/${encode(teamId)}/apps`,
        );
        all.push(...apps);
      }
      out(
        { apps: all },
        all.map((app) => `${app.appId}  ${app.name}  (team ${app.teamId})\n`).join("") ||
          "No apps.\n",
      );
      return 0;
    }

    case "create": {
      allowOnly(parsed, ["--team", "--region"]);
      const name = parsed.positionals[2];
      if (!name)
        throw new UsageError(
          "Usage: jazz-tools apps create <name> [--team <id>] [--region <region>]",
        );
      const teamId = await resolveTeam(api, flag(parsed, "--team"));
      const created = await api.request<{
        appId: string;
        teamId: string;
        name: string;
        region: string;
        adminSecret: string;
        backendSecret: string;
      }>("POST", `/teams/${encode(teamId)}/apps`, { name, region: flag(parsed, "--region") });
      out(created, `Created ${created.name} in ${created.region}.\n${secretsText(created)}`);
      return 0;
    }

    case "get": {
      allowOnly(parsed, []);
      const appId = appIdArg(parsed, 2, io.env);
      const app = await api.request<{
        appId: string;
        teamId: string;
        name: string;
        region: string;
        serverVersion: string;
        releaseLifecycle: string;
        status: string;
        serverUrl: string | null;
        auth: AppAuth;
      }>("GET", `/apps/${encode(appId)}`);
      out(
        app,
        `${app.name} (${app.appId})\n` +
          `  team:               ${app.teamId}\n` +
          `  region:             ${app.region}\n` +
          `  server version:     ${app.serverVersion} (${app.releaseLifecycle})\n` +
          `  status:             ${app.status}\n` +
          (app.serverUrl ? `  server URL:         ${app.serverUrl}\n` : "") +
          describeAuth(app.auth),
      );
      return 0;
    }

    case "claim": {
      allowOnly(parsed, ["--team", "--name", "--admin-secret"]);
      const appId = appIdArg(parsed, 2, io.env);
      const name = flag(parsed, "--name");
      if (!name) throw new UsageError("--name is required.");
      const adminSecret = flag(parsed, "--admin-secret") ?? io.env.JAZZ_ADMIN_SECRET?.trim();
      if (!adminSecret)
        throw new UsageError("--admin-secret is required (or set JAZZ_ADMIN_SECRET).");
      const teamId = await resolveTeam(api, flag(parsed, "--team"));
      const claimed = await api.request<{
        appId: string;
        teamId: string;
        name: string;
        wasAlreadyOwned: boolean;
      }>("POST", `/teams/${encode(teamId)}/apps/claim`, { appId, adminSecret, name });
      out(
        claimed,
        claimed.wasAlreadyOwned
          ? `${claimed.name} already belongs to team ${claimed.teamId}.\n`
          : `Claimed ${claimed.name} into team ${claimed.teamId}.\n`,
      );
      return 0;
    }

    case "delete": {
      allowOnly(parsed, ["--confirm"]);
      const appId = appIdArg(parsed, 2, io.env);
      const confirmName = flag(parsed, "--confirm");
      if (!confirmName) {
        throw new UsageError("Deleting an app needs --confirm <app name>. This cannot be undone.");
      }
      const deleted = await api.request<{ appId: string; teamId: string }>(
        "DELETE",
        `/apps/${encode(appId)}`,
        {
          confirmName,
        },
      );
      out(deleted, `Deleting app ${deleted.appId}.\n`);
      return 0;
    }

    case "auth":
      return await appAuth(parsed, io, api, out);

    case "secrets": {
      if (parsed.positionals[2] !== "rotate")
        throw new UsageError("Usage: jazz-tools apps secrets rotate <appId>");
      allowOnly(parsed, []);
      const appId = appIdArg(parsed, 3, io.env);
      const rotated = await api.request<{
        appId: string;
        adminSecret: string;
        backendSecret: string;
      }>("POST", `/apps/${encode(appId)}/secrets/rotate`);
      out(
        rotated,
        `Rotated secrets for ${rotated.appId}. The old secrets stop working once the app reconciles.\n${secretsText(rotated)}`,
      );
      return 0;
    }

    case "status": {
      allowOnly(parsed, ["--wait"]);
      const appId = appIdArg(parsed, 2, io.env);
      const sleep = io.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
      let status = await api.request<{
        ready: boolean;
        status: string;
        serverVersion: string;
        desiredVersion: number;
      }>("GET", `/apps/${encode(appId)}/status`);
      while (parsed.flags.has("--wait") && !status.ready) {
        await sleep(2000);
        status = await api.request("GET", `/apps/${encode(appId)}/status`);
      }
      out(
        status,
        `${status.status}${status.ready ? " (ready)" : ""}: server ${status.serverVersion}, config version ${status.desiredVersion}\n`,
      );
      return 0;
    }

    case "upgrade": {
      allowOnly(parsed, ["--release", "--expected-version", "--acknowledge-client-upgrade"]);
      const appId = appIdArg(parsed, 2, io.env);
      const releaseId = flag(parsed, "--release");
      if (!releaseId) {
        const status = await api.request<{
          currentVersion: string;
          available: Array<{
            releaseId: string;
            targetVersion: string;
            deadline: string;
            clientInstructions: string;
          }>;
        }>("GET", `/apps/${encode(appId)}/upgrade`);
        out(
          status,
          `Current version ${status.currentVersion}.\n` +
            (status.available.length === 0
              ? "No upgrades available.\n"
              : status.available
                  .map(
                    (release) =>
                      `  ${release.releaseId}  -> ${release.targetVersion} (deadline ${release.deadline})\n` +
                      `    ${release.clientInstructions}\n`,
                  )
                  .join("")),
        );
        return 0;
      }
      const expectedVersion = flag(parsed, "--expected-version");
      if (!expectedVersion) throw new UsageError("--expected-version is required with --release.");
      if (!parsed.flags.has("--acknowledge-client-upgrade")) {
        throw new UsageError(
          "Pass --acknowledge-client-upgrade to confirm you are coordinating the client upgrade.",
        );
      }
      const result = await api.request<{ desiredVersion: number }>(
        "POST",
        `/apps/${encode(appId)}/upgrade`,
        {
          releaseId,
          expectedVersion,
          acknowledgeClientUpgrade: true,
        },
      );
      out(
        result,
        `Upgrade started (config version ${result.desiredVersion}). Follow it with \`jazz-tools apps status ${appId} --wait\`.\n`,
      );
      return 0;
    }

    default:
      throw new UsageError(`Unknown command: apps ${sub ?? ""}`.trim());
  }
}

async function appAuth(
  parsed: ParsedArgs,
  io: CloudCommandIo,
  api: CloudApi,
  out: (value: unknown, human: string) => void,
): Promise<number> {
  const action = parsed.positionals[2];
  const appId = appIdArg(parsed, 3, io.env);
  if (action === "get") {
    allowOnly(parsed, []);
    const result = await api.request<{ auth: AppAuth }>(
      "GET",
      `/apps/${encodeURIComponent(appId)}/auth`,
    );
    out(result, describeAuth(result.auth));
    return 0;
  }
  if (action !== "set")
    throw new UsageError("Usage: jazz-tools apps auth get|set <appId> [options]");

  allowOnly(parsed, [
    "--jwks-url",
    "--jwt-issuer",
    "--jwt-audience",
    "--jwt-public-key",
    "--jwt-public-key-file",
    "--allow-local-first-auth",
  ]);
  const body: Record<string, unknown> = {};
  const map: Array<[string, string]> = [
    ["--jwks-url", "jwksUrl"],
    ["--jwt-issuer", "jwtIssuer"],
    ["--jwt-audience", "jwtAudience"],
    ["--jwt-public-key", "jwtPublicKey"],
  ];
  for (const [name, key] of map) {
    const value = flag(parsed, name);
    if (value !== undefined) body[key] = value;
  }
  const keyFile = flag(parsed, "--jwt-public-key-file");
  if (keyFile !== undefined) {
    if (body.jwtPublicKey !== undefined) {
      throw new UsageError("Use either --jwt-public-key or --jwt-public-key-file.");
    }
    body.jwtPublicKey = (await readFile(keyFile, "utf8")).trim();
  }
  const localFirst = flag(parsed, "--allow-local-first-auth");
  if (localFirst !== undefined) {
    if (localFirst !== "true" && localFirst !== "false") {
      throw new UsageError("--allow-local-first-auth must be true or false.");
    }
    body.allowLocalFirstAuth = localFirst === "true";
  }
  if (Object.keys(body).length === 0)
    throw new UsageError("Nothing to change. See `jazz-tools apps --help`.");
  const result = await api.request<{ auth: AppAuth; desiredVersion: number | null }>(
    "PATCH",
    `/apps/${encodeURIComponent(appId)}/auth`,
    body,
  );
  out(result, `Auth settings updated; the app is reconciling.\n${describeAuth(result.auth)}`);
  return 0;
}

function reportError(error: unknown, io: CloudCommandIo, json: boolean): number {
  const code =
    error instanceof CloudApiError || error instanceof CloudAuthError
      ? error.code
      : error instanceof UsageError
        ? "usage"
        : "error";
  const message = error instanceof Error ? error.message : String(error);
  if (json) {
    io.stdout(`${JSON.stringify({ error: code, message })}\n`);
  } else {
    io.stderr(`${message}\n`);
    if (error instanceof UsageError) io.stderr("Run `jazz-tools apps --help` for usage.\n");
  }
  return error instanceof UsageError ? 2 : 1;
}
