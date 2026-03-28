import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadCompiledSchema } from "../schema-loader.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../runtime/schema-fetch.js";

const DEFAULT_APP_ID = "00000000-0000-0000-0000-000000000001";
const DEFAULT_PORT = 1625;
const HEALTH_POLL_INTERVAL_MS = 100;
const HEALTH_REQUEST_TIMEOUT_MS = 1_000;
const DEFAULT_HEALTH_TIMEOUT_MS = 30_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 2_000;
const DEFAULT_STDERR_MAX_CHARS = 8_192;

const activeServers = new Set<{
  child: ChildProcess;
  ownsDataDir: boolean;
  dataDir: string;
}>();

let installedProcessHooks = false;

export interface StartLocalJazzServerOptions {
  appId?: string;
  port?: number;
  dataDir?: string;
  jwksUrl?: string;
  allowAnonymous?: boolean;
  allowDemo?: boolean;
  backendSecret?: string;
  adminSecret?: string;
  binaryPath?: string;
  healthTimeoutMs?: number;
  enableLogs?: boolean;
}

export interface LocalJazzServerHandle {
  appId: string;
  port: number;
  url: string;
  child: ChildProcess;
  dataDir: string;
  adminSecret?: string;
  backendSecret?: string;
  stop: () => Promise<void>;
}

export interface PushSchemaCatalogueOptions {
  serverUrl: string;
  appId: string;
  adminSecret: string;
  schemaDir: string;
  env?: string;
  userBranch?: string;
  binaryPath?: string;
  enableLogs?: boolean;
}

function defaultBinaryPath(): string {
  return join(import.meta.dirname ?? __dirname, "../../../../target/debug/jazz-tools");
}

function formatServerError(message: string, stderrText: string): Error {
  if (stderrText.trim().length === 0) {
    return new Error(message);
  }
  return new Error(`${message}\n\nstderr:\n${stderrText}`);
}

async function isHealthy(port: number): Promise<boolean> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), HEALTH_REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(`http://127.0.0.1:${port}/health`, {
      signal: controller.signal,
    });
    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
  }
}

function installProcessHooks(): void {
  if (installedProcessHooks) {
    return;
  }
  installedProcessHooks = true;

  const cleanupAllServers = () => {
    for (const server of activeServers) {
      if (server.child.exitCode === null && server.child.signalCode === null) {
        try {
          server.child.kill("SIGTERM");
        } catch {
          // Best effort cleanup.
        }
      }

      if (server.ownsDataDir) {
        try {
          rmSync(server.dataDir, { recursive: true, force: true });
        } catch {
          // Best effort cleanup.
        }
      }
    }
    activeServers.clear();
  };

  process.once("exit", cleanupAllServers);
  process.once("SIGINT", () => {
    cleanupAllServers();
    process.exit(130);
  });
  process.once("SIGTERM", () => {
    cleanupAllServers();
    process.exit(143);
  });
}

async function waitForExit(child: ChildProcess, timeoutMs: number): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }

  await new Promise<void>((resolve) => {
    const timeout = setTimeout(() => resolve(), timeoutMs);

    const onExit = () => {
      clearTimeout(timeout);
      resolve();
    };

    child.once("exit", onExit);
  });
}

export async function startLocalJazzServer(
  options: StartLocalJazzServerOptions = {},
): Promise<LocalJazzServerHandle> {
  const appId = options.appId ?? DEFAULT_APP_ID;
  const port = options.port ?? DEFAULT_PORT;
  const healthTimeoutMs = options.healthTimeoutMs ?? DEFAULT_HEALTH_TIMEOUT_MS;
  const binaryPath = options.binaryPath ?? defaultBinaryPath();
  const ownsDataDir = options.dataDir === undefined;
  const dataDir = options.dataDir ?? mkdtempSync(join(tmpdir(), "jazz-tools-local-server-"));
  const enableLogs = options.enableLogs === true;

  if (!existsSync(binaryPath)) {
    throw new Error(
      `jazz-tools binary not found at ${binaryPath}. Run \`cargo build -p jazz-tools --bin jazz-tools --features cli\` first.`,
    );
  }

  installProcessHooks();

  const args = ["server", appId, "--port", String(port), "--data-dir", dataDir];
  if (options.jwksUrl !== undefined) {
    args.push("--jwks-url", options.jwksUrl);
  }
  if (options.allowAnonymous === true) {
    args.push("--allow-anonymous");
  }
  if (options.allowDemo === true) {
    args.push("--allow-demo");
  }
  if (options.backendSecret !== undefined) {
    args.push("--backend-secret", options.backendSecret);
  }
  if (options.adminSecret !== undefined) {
    args.push("--admin-secret", options.adminSecret);
  }

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    NODE_ENV: process.env.NODE_ENV ?? "test",
  };
  if (options.jwksUrl !== undefined) {
    env.JAZZ_JWKS_URL = options.jwksUrl;
  }
  if (options.backendSecret !== undefined) {
    env.JAZZ_BACKEND_SECRET = options.backendSecret;
  }
  if (options.adminSecret !== undefined) {
    env.JAZZ_ADMIN_SECRET = options.adminSecret;
  }
  if (options.allowAnonymous === true) {
    env.JAZZ_ALLOW_ANONYMOUS = "1";
  }
  if (options.allowDemo === true) {
    env.JAZZ_ALLOW_DEMO = "1";
  }

  const child = spawn(binaryPath, args, {
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stderrText = "";
  child.stdout?.on("data", (chunk: Buffer) => {
    if (enableLogs) {
      process.stdout.write(`[jazz-server] ${chunk}`);
    }
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    if (enableLogs) {
      process.stderr.write(`[jazz-server] ${chunk}`);
    }
    stderrText += chunk.toString("utf8");
    if (stderrText.length > DEFAULT_STDERR_MAX_CHARS) {
      stderrText = stderrText.slice(-DEFAULT_STDERR_MAX_CHARS);
    }
  });

  const activeServerEntry = { child, ownsDataDir, dataDir };
  activeServers.add(activeServerEntry);
  const removeActiveEntry = () => activeServers.delete(activeServerEntry);
  child.once("exit", removeActiveEntry);

  const startupError = await new Promise<Error | null>((resolve) => {
    let settled = false;
    const deadline = Date.now() + healthTimeoutMs;

    const finish = (error: Error | null) => {
      if (settled) {
        return;
      }
      settled = true;
      resolve(error);
    };

    const onError = (error: Error) => {
      finish(
        formatServerError(
          `Local jazz server failed before health check succeeded: ${error.message}`,
          stderrText,
        ),
      );
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      finish(
        formatServerError(
          `Local jazz server exited before health check succeeded (code=${code ?? "null"}, signal=${
            signal ?? "null"
          }).`,
          stderrText,
        ),
      );
    };

    child.once("error", onError);
    child.once("exit", onExit);

    const pollHealth = async () => {
      while (!settled) {
        if (await isHealthy(port)) {
          if (child.exitCode !== null || child.signalCode !== null) {
            finish(
              formatServerError(
                "Local jazz server reported healthy on port, but child process already exited.",
                stderrText,
              ),
            );
            return;
          }

          child.off("error", onError);
          child.off("exit", onExit);
          finish(null);
          return;
        }
        if (Date.now() >= deadline) {
          finish(
            formatServerError(
              `Local jazz server did not become healthy within ${healthTimeoutMs}ms.`,
              stderrText,
            ),
          );
          return;
        }
        await new Promise((resolvePoll) => setTimeout(resolvePoll, HEALTH_POLL_INTERVAL_MS));
      }
    };

    void pollHealth();
  });

  if (startupError) {
    if (child.exitCode === null && child.signalCode === null) {
      try {
        child.kill("SIGTERM");
      } catch {
        // Best effort cleanup.
      }
      await waitForExit(child, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }
    activeServers.delete(activeServerEntry);
    if (ownsDataDir) {
      try {
        rmSync(dataDir, { recursive: true, force: true });
      } catch {
        // Best effort cleanup.
      }
    }
    throw startupError;
  }

  let stopped = false;
  let stopPromise: Promise<void> | null = null;

  const stop = async () => {
    if (stopped) {
      return;
    }
    if (stopPromise) {
      return stopPromise;
    }

    stopPromise = (async () => {
      if (child.exitCode === null && child.signalCode === null) {
        try {
          child.kill("SIGTERM");
        } catch {
          // Best effort cleanup.
        }
        await waitForExit(child, DEFAULT_SHUTDOWN_TIMEOUT_MS);
      }

      activeServers.delete(activeServerEntry);
      if (ownsDataDir) {
        try {
          rmSync(dataDir, { recursive: true, force: true });
        } catch {
          // Best effort cleanup.
        }
      }
      stopped = true;
    })();

    return stopPromise;
  };

  return {
    appId,
    port,
    url: `http://127.0.0.1:${port}`,
    child,
    dataDir,
    adminSecret: options.adminSecret,
    backendSecret: options.backendSecret,
    stop,
  };
}

export async function pushSchemaCatalogue(options: PushSchemaCatalogueOptions): Promise<void> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  const result = await publishStoredSchema(options.serverUrl, {
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
  });

  if (compiled.permissions) {
    const { head } = await fetchPermissionsHead(options.serverUrl, {
      adminSecret: options.adminSecret,
    });
    await publishStoredPermissions(options.serverUrl, {
      adminSecret: options.adminSecret,
      schemaHash: result.hash,
      permissions: compiled.permissions,
      expectedParentBundleObjectId: head?.bundleObjectId ?? null,
    });
  }

  if (options.enableLogs === true) {
    console.log(
      `[jazz-schema-push] published ${result.hash} from ${compiled.schemaFile} to ${options.serverUrl}`,
    );
  }
}
