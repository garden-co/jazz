import { spawn, type ChildProcess } from "node:child_process";
import {
  createServer,
  request as httpRequest,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { existsSync } from "node:fs";
import { createServer as createNetServer, type AddressInfo } from "node:net";
import { fileURLToPath } from "node:url";

export interface ReverseProxyServerOptions {
  authOrigin: string;
  bindHost?: string;
  port: number;
  syncOrigin: string;
}

export interface ReverseProxyServerHandle {
  close(): Promise<void>;
  port: number;
  url: string;
}

export interface ParsedCombinedServerCommand {
  appId: string;
  authMode: "bundled" | "none";
  explicitJwksUrl?: string;
  publicPort: number;
  rustArgs: string[];
}

export interface RunCombinedServerCommandOptions {
  env?: NodeJS.ProcessEnv;
  rustBinaryPath?: string;
}

const DEFAULT_BIND_HOST = "127.0.0.1";
const DEFAULT_PUBLIC_PORT = 1625;
const DEFAULT_HEALTH_TIMEOUT_MS = 30_000;
const HEALTH_POLL_INTERVAL_MS = 100;

export function isAuthOwnedPath(pathname: string): boolean {
  return (
    pathname === "/auth" ||
    pathname.startsWith("/auth/") ||
    pathname === "/api/auth" ||
    pathname.startsWith("/api/auth/") ||
    pathname === "/.well-known/jwks.json" ||
    pathname === "/jwks"
  );
}

function getFlagValue(args: string[], flag: string): string | undefined {
  for (let index = 0; index < args.length; index += 1) {
    const value = args[index];
    if (!value) {
      continue;
    }

    if (value === flag) {
      return args[index + 1];
    }

    const prefix = `${flag}=`;
    if (value.startsWith(prefix)) {
      return value.slice(prefix.length);
    }
  }

  return undefined;
}

function withoutAuthFlags(args: string[]): string[] {
  const filtered: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const value = args[index];
    if (!value) {
      continue;
    }

    if (value === "--auth") {
      index += 1;
      continue;
    }

    if (value.startsWith("--auth=")) {
      continue;
    }

    filtered.push(value);
  }

  return filtered;
}

function replaceOrAppendFlag(args: string[], flag: string, nextValue: string): string[] {
  const updated = [...args];

  for (let index = 0; index < updated.length; index += 1) {
    const value = updated[index];
    if (!value) {
      continue;
    }

    if (value === flag) {
      updated[index + 1] = nextValue;
      return updated;
    }

    const prefix = `${flag}=`;
    if (value.startsWith(prefix)) {
      updated[index] = `${flag}=${nextValue}`;
      return updated;
    }
  }

  updated.push(flag, nextValue);
  return updated;
}

export function parseCombinedServerCommand(rawArgs: string[]): ParsedCombinedServerCommand {
  if (rawArgs[0] !== "server") {
    throw new Error("Combined server runner expects arguments starting with `server`.");
  }

  const appId = rawArgs[1];
  if (!appId || appId.startsWith("-")) {
    throw new Error("Missing app ID. Usage: jazz-tools server <APP_ID> [options]");
  }

  const serverArgs = rawArgs.slice(2);
  const portValue = getFlagValue(serverArgs, "--port") ?? getFlagValue(serverArgs, "-p");
  const authModeValue = getFlagValue(serverArgs, "--auth") ?? "bundled";
  const authMode =
    authModeValue === "none" || authModeValue === "bundled"
      ? authModeValue
      : (() => {
          throw new Error(`Unsupported auth mode: ${authModeValue}`);
        })();

  return {
    appId,
    authMode,
    explicitJwksUrl: getFlagValue(serverArgs, "--jwks-url"),
    publicPort: portValue ? Number.parseInt(portValue, 10) : DEFAULT_PUBLIC_PORT,
    rustArgs: withoutAuthFlags(rawArgs),
  };
}

function printCombinedServerHelp(): void {
  console.log("Usage: jazz-tools server <APP_ID> [options]");
  console.log("");
  console.log("Options:");
  console.log("  --port <port>         Public port for the combined server (default: 1625)");
  console.log("  --auth <mode>         Auth mode: bundled or none (default: bundled)");
  console.log("  --data-dir <path>     Data directory forwarded to the Rust sync server");
  console.log("  --jwks-url <url>      External JWKS URL (requires --auth=none)");
  console.log("  -h, --help            Print help");
}

function buildUpstreamUrl(origin: string, requestPath: string): URL {
  return new URL(requestPath || "/", origin);
}

function proxyRequest(request: IncomingMessage, response: ServerResponse, target: URL): void {
  const forwardedHeaders: Record<string, string | string[]> = {};

  for (const [key, value] of Object.entries(request.headers)) {
    if (value === undefined) {
      continue;
    }

    if (key.toLowerCase() === "host") {
      continue;
    }

    forwardedHeaders[key] = value;
  }

  if (request.headers.host) {
    forwardedHeaders["x-forwarded-host"] = request.headers.host;
  }
  forwardedHeaders["x-forwarded-proto"] = target.protocol.replace(/:$/, "");
  forwardedHeaders.host = target.host;

  const upstreamRequest = httpRequest(
    {
      headers: forwardedHeaders,
      hostname: target.hostname,
      method: request.method,
      path: `${target.pathname}${target.search}`,
      port: target.port,
      protocol: target.protocol,
    },
    (upstreamResponse) => {
      response.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers);
      upstreamResponse.pipe(response);
    },
  );

  upstreamRequest.on("error", (error) => {
    if (response.headersSent) {
      response.destroy(error);
      return;
    }

    response.writeHead(502, { "content-type": "application/json; charset=utf-8" });
    response.end(JSON.stringify({ error: "upstream_proxy_failed", message: error.message }));
  });

  request.pipe(upstreamRequest);
}

export async function startReverseProxyServer(
  options: ReverseProxyServerOptions,
): Promise<ReverseProxyServerHandle> {
  const bindHost = options.bindHost ?? DEFAULT_BIND_HOST;
  const authOrigin = new URL(options.authOrigin);
  const syncOrigin = new URL(options.syncOrigin);

  const server = createServer((request, response) => {
    const parsed = new URL(request.url ?? "/", "http://jazz-tools.local");
    const target = isAuthOwnedPath(parsed.pathname)
      ? buildUpstreamUrl(authOrigin.toString(), `${parsed.pathname}${parsed.search}`)
      : buildUpstreamUrl(syncOrigin.toString(), `${parsed.pathname}${parsed.search}`);

    proxyRequest(request, response, target);
  });

  await new Promise<void>((resolve, reject) => {
    server.listen(options.port, bindHost, (error?: Error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });

  const address = server.address() as AddressInfo;

  return {
    async close(): Promise<void> {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
    port: address.port,
    url: `http://${bindHost}:${address.port}`,
  };
}

function defaultRustBinaryPath(): string {
  return fileURLToPath(new URL("../../../../target/debug/jazz-tools", import.meta.url));
}

async function reservePort(bindHost: string = DEFAULT_BIND_HOST): Promise<number> {
  const server = createNetServer();

  await new Promise<void>((resolve, reject) => {
    server.listen(0, bindHost, (error?: Error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });

  const address = server.address() as AddressInfo;
  const port = address.port;

  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });

  return port;
}

async function waitForHealthy(
  url: string,
  timeoutMs: number = DEFAULT_HEALTH_TIMEOUT_MS,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {}

    await new Promise((resolve) => setTimeout(resolve, HEALTH_POLL_INTERVAL_MS));
  }

  throw new Error(`Timed out waiting for ${url} to become healthy.`);
}

async function waitForChildExit(child: ChildProcess): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (typeof code === "number") {
        resolve(code);
        return;
      }

      if (signal) {
        resolve(1);
        return;
      }

      resolve(0);
    });
  });
}

async function terminateChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }

  child.kill("SIGTERM");

  await new Promise<void>((resolve) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      resolve();
    }, 2_000);

    child.once("exit", () => {
      clearTimeout(timeout);
      resolve();
    });
  });
}

function spawnRustServer(binaryPath: string, args: string[], env: NodeJS.ProcessEnv): ChildProcess {
  return spawn(binaryPath, args, {
    env,
    stdio: "inherit",
  });
}

async function loadVendoredJazzAuthServer(): Promise<{
  startJazzHostedAuthServer: (options: {
    baseURL: string;
    bindHost?: string;
    port?: number;
    secret: string;
  }) => Promise<{ close(): Promise<void>; port: number; url: string }>;
}> {
  const moduleUrl = new URL("./vendor/jazz-auth/hosted/server.js", import.meta.url);
  return (await import(moduleUrl.href)) as {
    startJazzHostedAuthServer: (options: {
      baseURL: string;
      bindHost?: string;
      port?: number;
      secret: string;
    }) => Promise<{ close(): Promise<void>; port: number; url: string }>;
  };
}

async function runRustOnlyServer(
  parsed: ParsedCombinedServerCommand,
  options: RunCombinedServerCommandOptions,
): Promise<number> {
  const rustBinaryPath =
    options.rustBinaryPath ?? options.env?.JAZZ_TOOLS_RUST_BIN ?? defaultRustBinaryPath();

  if (!existsSync(rustBinaryPath)) {
    throw new Error(`Rust server binary not found at ${rustBinaryPath}.`);
  }

  const child = spawnRustServer(rustBinaryPath, parsed.rustArgs, {
    ...process.env,
    ...options.env,
  });

  return waitForChildExit(child);
}

export async function runCombinedServerCommand(
  rawArgs: string[],
  options: RunCombinedServerCommandOptions = {},
): Promise<number> {
  const parsed = parseCombinedServerCommand(rawArgs);

  if (parsed.authMode === "none") {
    return runRustOnlyServer(parsed, options);
  }

  if (parsed.explicitJwksUrl) {
    throw new Error(
      "Bundled Jazz Auth cannot be combined with --jwks-url. Use --auth=none to keep an external JWKS issuer.",
    );
  }

  const rustBinaryPath =
    options.rustBinaryPath ?? options.env?.JAZZ_TOOLS_RUST_BIN ?? defaultRustBinaryPath();
  if (!existsSync(rustBinaryPath)) {
    throw new Error(`Rust server binary not found at ${rustBinaryPath}.`);
  }

  const publicBaseURL =
    options.env?.JAZZ_AUTH_BASE_URL ??
    process.env.JAZZ_AUTH_BASE_URL ??
    `http://${DEFAULT_BIND_HOST}:${parsed.publicPort}`;
  const authSecret =
    options.env?.JAZZ_AUTH_SECRET ?? process.env.JAZZ_AUTH_SECRET ?? "jazz-auth-development-secret";
  const syncPort = await reservePort();
  const authPort = await reservePort();

  const { startJazzHostedAuthServer } = await loadVendoredJazzAuthServer();
  const authServer = await startJazzHostedAuthServer({
    baseURL: publicBaseURL,
    bindHost: DEFAULT_BIND_HOST,
    port: authPort,
    secret: authSecret,
  });

  let rustChild: ChildProcess | null = null;
  let proxy: ReverseProxyServerHandle | null = null;

  const cleanup = async () => {
    await Promise.allSettled([
      proxy?.close(),
      authServer.close(),
      rustChild ? terminateChild(rustChild) : Promise.resolve(),
    ]);
  };

  const onSignal = async (signal: NodeJS.Signals) => {
    process.off("SIGINT", onSigInt);
    process.off("SIGTERM", onSigTerm);
    await cleanup();
    process.exit(signal === "SIGINT" ? 130 : 143);
  };

  const onSigInt = () => {
    void onSignal("SIGINT");
  };
  const onSigTerm = () => {
    void onSignal("SIGTERM");
  };

  process.once("SIGINT", onSigInt);
  process.once("SIGTERM", onSigTerm);

  try {
    const syncOrigin = `http://${DEFAULT_BIND_HOST}:${syncPort}`;
    const rustArgsWithPort = replaceOrAppendFlag(parsed.rustArgs, "--port", String(syncPort));
    const rustArgs = replaceOrAppendFlag(
      rustArgsWithPort,
      "--jwks-url",
      `${authServer.url}/.well-known/jwks.json`,
    );

    rustChild = spawnRustServer(rustBinaryPath, rustArgs, {
      ...process.env,
      ...options.env,
    });

    await Promise.all([
      waitForHealthy(`${syncOrigin}/health`),
      waitForHealthy(`${authServer.url}/health`),
    ]);

    proxy = await startReverseProxyServer({
      authOrigin: authServer.url,
      bindHost: DEFAULT_BIND_HOST,
      port: parsed.publicPort,
      syncOrigin,
    });

    console.log(`Jazz server listening on ${proxy.url}`);
    console.log(`Bundled Jazz Auth available at ${proxy.url}/auth/sign-in`);

    const exitCode = await waitForChildExit(rustChild);
    await cleanup();
    process.off("SIGINT", onSigInt);
    process.off("SIGTERM", onSigTerm);
    return exitCode;
  } catch (error) {
    process.off("SIGINT", onSigInt);
    process.off("SIGTERM", onSigTerm);
    await cleanup();
    throw error;
  }
}

async function main(): Promise<void> {
  try {
    const rawArgs = process.argv.slice(2);
    const wantsHelp =
      rawArgs[0] === "server" && (rawArgs.includes("--help") || rawArgs.includes("-h"));
    if (wantsHelp) {
      printCombinedServerHelp();
      process.exit(0);
    }

    const exitCode = await runCombinedServerCommand(rawArgs, {
      env: process.env,
      rustBinaryPath: process.env.JAZZ_TOOLS_RUST_BIN,
    });
    process.exit(exitCode);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

const currentFilePath = fileURLToPath(import.meta.url);

if (process.argv[1] && currentFilePath === process.argv[1]) {
  void main();
}
