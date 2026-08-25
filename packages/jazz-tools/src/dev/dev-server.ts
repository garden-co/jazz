import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JazzServer } from "jazz-napi";

export { deploy, type DeployOptions } from "./catalogue.js";

const DEFAULT_APP_ID = "00000000-0000-0000-0000-000000000001";

export interface StartLocalJazzServerOptions {
  appId?: string;
  port?: number;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  backendSecret?: string;
  adminSecret?: string;
  upstreamUrl?: string;
  allowLocalFirstAuth?: boolean;
  telemetryCollectorUrl?: string;
  enableLogs?: boolean;
  schema?: Uint8Array;
}

export interface LocalJazzServerHandle {
  appId: string;
  port: number;
  url: string;
  dataDir: string;
  adminSecret: string;
  backendSecret: string;
  stop: () => Promise<void>;
}

async function createOwnedDataDir(): Promise<string> {
  return await mkdtemp(join(tmpdir(), "jazz-dev-server-"));
}

/**
 * Start a local Jazz sync server.
 *
 * When no port is provided, an available localhost port is chosen automatically.
 * When no data directory is provided, the server owns a temporary directory and
 * removes it when {@link LocalJazzServerHandle.stop} is called. Pass
 * `inMemory: true` for an in-memory server instead. Admin and backend secrets
 * are generated when omitted.
 *
 * @returns A handle with the server URL, resolved app id, secrets, and an
 * idempotent `stop()` method that shuts the server down and releases owned
 * resources.
 */
export async function startLocalJazzServer(
  options: StartLocalJazzServerOptions = {},
): Promise<LocalJazzServerHandle> {
  const appId = options.appId ?? DEFAULT_APP_ID;
  // Ask the server to bind port 0 when callers do not require a particular
  // address. Unlike probing a candidate port first, this is atomic and remains
  // safe across Vitest worker processes (and separate concurrent CI jobs).
  const port = options.port ?? 0;
  const ownsDataDir = options.inMemory !== true && options.dataDir === undefined;
  const dataDir = ownsDataDir ? await createOwnedDataDir() : options.dataDir;
  const adminSecret = options.adminSecret ?? `jazz-test-admin-${randomUUID().slice(0, 8)}`;
  const backendSecret = options.backendSecret ?? `jazz-test-backend-${randomUUID().slice(0, 8)}`;

  let server;
  try {
    server = await JazzServer.start({
      appId,
      port,
      dataDir,
      inMemory: options.inMemory,
      jwksUrl: options.jwksUrl,
      backendSecret,
      adminSecret,
      upstreamUrl: options.upstreamUrl,
      allowLocalFirstAuth: options.allowLocalFirstAuth,
      telemetryCollectorUrl: options.telemetryCollectorUrl,
      schema: options.schema ? [...options.schema] : undefined,
    });
  } catch (error) {
    if (ownsDataDir && dataDir) {
      await rm(dataDir, { recursive: true, force: true }).catch(() => undefined);
    }
    throw error;
  }

  if (options.enableLogs === true) {
    console.log(`[jazz-server] started on ${server.url}`);
  }

  let stopPromise: Promise<void> | null = null;
  const stop = async () => {
    if (stopPromise) {
      return await stopPromise;
    }

    stopPromise = (async () => {
      try {
        await server.stop();
      } finally {
        if (ownsDataDir && dataDir) {
          await rm(dataDir, { recursive: true, force: true }).catch(() => undefined);
        }
      }
    })();

    return await stopPromise;
  };

  return {
    appId: server.appId,
    port: server.port,
    url: server.url,
    dataDir: server.dataDir,
    adminSecret: server.adminSecret,
    backendSecret: server.backendSecret,
    stop,
  };
}
