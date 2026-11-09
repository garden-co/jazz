// The Node side of the Inspector protocol: what `jazz-tools inspect` and
// `jazz-tools inspect mcp` share. It calls an app's backend at
// `<basePath>/inspect/api`, keeps the agent tokens a person approved, and
// runs the approval flow that gets one without editing any file.
//
// Approval: the CLI listens on a loopback port and opens the app's
// Inspector with a grant request in the URL fragment. The person signs in
// as they always do (the app's `inspect` hook decides), sees what is asked
// for, and approves; their browser then hands the token to the loopback
// port, which only this machine can reach. Where the browser runs on another
// machine, the Inspector shows the token instead, to pass to
// `jazz-tools inspect login --token`.

import { randomBytes } from "node:crypto";
import { createServer, type Server } from "node:http";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { configDir } from "../cloud/credentials.js";
import {
  INSPECT_API_PATH,
  INSPECT_PATH,
  INSPECT_TOKEN_PREFIX,
  InspectError,
  isLoopbackHost,
  type InspectErrorAnswer,
  type InspectGrantRequest,
  type InspectRequest,
} from "./protocol.js";

/** The app URL used when none is given: a local dev server. */
export const DEFAULT_APP_URL = "http://localhost:3000";
const DEFAULT_BASE_PATH = "/jazz";

/**
 * The endpoint (origin and Jazz `basePath`) for what a person typed: an app's
 * URL (`https://app.example.com`, Jazz under `/jazz`), its Jazz URL
 * (`https://app.example.com/api/jazz`), or an Inspector link.
 */
export function resolveEndpoint(input: string): string {
  let url: URL;
  try {
    url = new URL(/^[a-z]+:\/\//i.test(input) ? input : `http://${input}`);
  } catch {
    throw new InspectError("bad_request", `${JSON.stringify(input)} is not a URL`);
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new InspectError("bad_request", `${input} is not an http(s) URL`);
  }
  let path = url.pathname.replace(/\/+$/, "");
  for (const suffix of [INSPECT_API_PATH, INSPECT_PATH]) {
    if (path.endsWith(suffix)) path = path.slice(0, -suffix.length);
  }
  return `${url.origin}${path || DEFAULT_BASE_PATH}`;
}

export interface StoredToken {
  token: string;
  /** Unix seconds. */
  expiresAt: number;
  by?: string;
  write?: boolean;
  tables?: string[];
}

interface TokenFile {
  version: 1;
  tokens: Record<string, StoredToken>;
}

/** The environment variables the inspect tools read. */
export type InspectEnv = Readonly<Record<string, string | undefined>>;

export function tokenStorePath(env: InspectEnv = process.env): string {
  return join(configDir(env), "inspect-tokens.json");
}

async function readTokenFile(path: string): Promise<TokenFile> {
  try {
    const parsed = JSON.parse(await readFile(path, "utf8")) as TokenFile;
    if (parsed?.version === 1 && parsed.tokens && typeof parsed.tokens === "object") return parsed;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT" && !(error instanceof SyntaxError)) {
      throw error;
    }
  }
  return { version: 1, tokens: {} };
}

async function writeTokenFile(path: string, file: TokenFile): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temp = `${path}.${randomBytes(6).toString("hex")}.tmp`;
  await writeFile(temp, `${JSON.stringify(file, null, 2)}\n`, { mode: 0o600 });
  await chmod(temp, 0o600);
  await rename(temp, path);
}

/** Agent tokens on this machine, by endpoint. */
export function tokenStore(env: InspectEnv = process.env, now: () => number = Date.now) {
  const path = tokenStorePath(env);
  return {
    path,
    /** The endpoint's token, unless it expired. `JAZZ_INSPECT_TOKEN` wins over a stored one. */
    async get(endpoint: string): Promise<StoredToken | null> {
      const fromEnv = env.JAZZ_INSPECT_TOKEN?.trim();
      if (fromEnv) return { token: fromEnv, expiresAt: Number.MAX_SAFE_INTEGER };
      const stored = (await readTokenFile(path)).tokens[endpoint];
      if (!stored || stored.expiresAt * 1000 <= now()) return null;
      return stored;
    },
    async set(endpoint: string, token: StoredToken): Promise<void> {
      const file = await readTokenFile(path);
      file.tokens[endpoint] = token;
      // Expired tokens are dead weight.
      for (const [key, entry] of Object.entries(file.tokens)) {
        if (entry.expiresAt * 1000 <= now()) delete file.tokens[key];
      }
      await writeTokenFile(path, file);
    },
    async remove(endpoint: string): Promise<boolean> {
      const file = await readTokenFile(path);
      if (!(endpoint in file.tokens)) return false;
      delete file.tokens[endpoint];
      await writeTokenFile(path, file);
      return true;
    },
  };
}

export type TokenStore = ReturnType<typeof tokenStore>;

/** Call the Inspector API at `endpoint`, with `token` when there is one. */
export async function callInspector<T = unknown>(
  endpoint: string,
  request: InspectRequest,
  options: { token?: string | null; fetch?: typeof fetch } = {},
): Promise<T> {
  const doFetch = options.fetch ?? fetch;
  // A token reads every row it's scoped to: it never crosses a network in the clear.
  if (options.token) {
    const url = new URL(endpoint);
    if (url.protocol !== "https:" && !isLoopbackHost(url.hostname)) {
      throw new InspectError(
        "bad_request",
        `won't send an agent token to ${endpoint} over plain http: use the app's https URL, or a loopback address in local development`,
      );
    }
  }
  let response: Response;
  try {
    response = await doFetch(`${endpoint}${INSPECT_API_PATH}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(options.token ? { authorization: `Bearer ${options.token}` } : {}),
      },
      body: JSON.stringify(request),
    });
  } catch (error) {
    throw new InspectError(
      "failed",
      `couldn't reach ${endpoint}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  const text = await response.text();
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    throw new InspectError(
      response.status === 404 ? "not_found" : "failed",
      response.status === 404
        ? `${endpoint}${INSPECT_API_PATH} not found: is Jazz mounted at ${new URL(endpoint).pathname}? Pass the URL Jazz is served under, e.g. https://app.example.com/api/jazz`
        : `unexpected answer (${response.status}) from ${endpoint}`,
    );
  }
  if (!response.ok) {
    const error = body as Partial<InspectErrorAnswer>;
    throw new InspectError(error.error ?? "failed", error.message ?? `HTTP ${response.status}`);
  }
  return body as T;
}

export type GrantRequest = Omit<InspectGrantRequest, "callback" | "state">;

export function encodeGrant(grant: InspectGrantRequest): string {
  return Buffer.from(JSON.stringify(grant)).toString("base64url");
}

export interface PendingApproval {
  /** The Inspector URL the person opens to approve. */
  url: string;
  /** Resolves with the token once the person approved, or rejects on timeout or `cancel`. */
  token: Promise<StoredToken>;
  cancel(): void;
}

/**
 * Ask a person to approve an agent token for `endpoint`: listen on a
 * loopback port, and answer with the Inspector URL to open. The token is
 * stored once the browser hands it over.
 */
export async function requestApproval(
  endpoint: string,
  grant: GrantRequest,
  options: { store: TokenStore; timeoutMs?: number; fetch?: typeof fetch },
): Promise<PendingApproval> {
  const state = randomBytes(18).toString("base64url");
  let server: Server | undefined;
  let settle!: {
    resolve: (token: StoredToken) => void;
    reject: (error: Error) => void;
  };
  const token = new Promise<StoredToken>((resolve, reject) => {
    settle = { resolve, reject };
  });
  // The promise may be dropped by a caller that only wants the URL.
  token.catch(() => {});
  let timer: ReturnType<typeof setTimeout> | undefined;
  const finish = () => {
    if (timer) clearTimeout(timer);
    server?.close();
    server?.closeAllConnections?.();
  };

  server = createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://127.0.0.1");
    if (url.pathname !== "/callback") {
      res.statusCode = 404;
      res.end();
      return;
    }
    const given = url.searchParams.get("token");
    if (url.searchParams.get("state") !== state || !given?.startsWith(INSPECT_TOKEN_PREFIX)) {
      res.statusCode = 400;
      res.end("This approval is not the one this terminal asked for.");
      return;
    }
    void accept(given).then(
      (stored) => {
        res.setHeader("content-type", "text/html; charset=utf-8");
        // Stop listening once the browser has its answer.
        res.on("finish", () => {
          finish();
          settle.resolve(stored);
        });
        res.end(
          "<!doctype html><title>Approved</title><p style='font:16px system-ui;margin:3rem'>Approved. The agent has its token; you can close this tab.</p>",
        );
      },
      (error: Error) => {
        res.statusCode = 400;
        res.end(error.message);
      },
    );
  });
  async function accept(given: string): Promise<StoredToken> {
    // Ask the backend what the token is: this also proves it works.
    const whoami = await callInspector<{
      by: string;
      write: boolean;
      tables?: string[];
      expiresAt?: number;
    }>(endpoint, { op: "whoami" }, { token: given, fetch: options.fetch });
    const stored: StoredToken = {
      token: given,
      expiresAt: whoami.expiresAt ?? Math.floor(Date.now() / 1000) + 3600,
      by: whoami.by,
      write: whoami.write,
      ...(whoami.tables ? { tables: whoami.tables } : {}),
    };
    await options.store.set(endpoint, stored);
    return stored;
  }

  await new Promise<void>((resolve, reject) => {
    server!.once("error", reject);
    server!.listen(0, "127.0.0.1", resolve);
  });
  server.unref();
  const port = (server.address() as { port: number }).port;
  timer = setTimeout(
    () => {
      finish();
      settle.reject(new InspectError("failed", "nobody approved the request in time"));
    },
    options.timeoutMs ?? 10 * 60_000,
  );
  (timer as unknown as { unref?: () => void }).unref?.();
  const fragment = encodeGrant({
    ...grant,
    callback: `http://127.0.0.1:${port}/callback`,
    state,
  });
  return {
    url: `${endpoint}${INSPECT_PATH}/#grant=${fragment}`,
    token,
    cancel() {
      finish();
      settle.reject(new InspectError("failed", "the approval request was cancelled"));
    },
  };
}

/** Parse a duration like `30m`, `1h`, `1d` or a number of seconds. */
export function parseDuration(text: string): number {
  const match = /^(\d+)\s*([smhd]?)$/.exec(text.trim());
  if (!match) throw new InspectError("bad_request", `${text} is not a duration like 30m, 1h or 1d`);
  const unit = { "": 1, s: 1, m: 60, h: 3600, d: 86400 }[match[2] as "" | "s" | "m" | "h" | "d"];
  return Number(match[1]) * unit;
}
