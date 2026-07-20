import { createServer as createNetServer } from "node:net";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JazzServer } from "jazz-napi";

export { deploy, type DeployOptions } from "./catalogue.js";

const DEFAULT_APP_ID = "00000000-0000-0000-0000-000000000001";
// The 20000-40000 space is partitioned by process id: vitest runs test files
// in separate worker processes whose module-global allocation state cannot
// coordinate, and a shared range let one worker steal a port another worker's
// reopen test had briefly released (the napi.integration reopen flakes).
const AUTO_PORT_SLOTS = 64;
const AUTO_PORT_SLOT = process.pid % AUTO_PORT_SLOTS;
const AUTO_PORT_RANGE = Math.floor(20_000 / AUTO_PORT_SLOTS);
const AUTO_PORT_MIN = 20_000 + AUTO_PORT_SLOT * AUTO_PORT_RANGE;

const autoAllocatedPorts = new Set<number>();

let nextAutoPort = AUTO_PORT_MIN + Math.floor(Math.random() * AUTO_PORT_RANGE);

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

async function canBindPort(port: number): Promise<boolean> {
  return await new Promise<boolean>((resolve) => {
    const server = createNetServer();
    server.once("error", () => {
      resolve(false);
    });
    server.listen(port, "127.0.0.1", () => {
      server.close((error) => {
        void error;
        resolve(true);
      });
    });
  });
}

async function allocateAutoPort(): Promise<number> {
  for (let attempts = 0; attempts < AUTO_PORT_RANGE; attempts += 1) {
    const candidate = nextAutoPort;
    nextAutoPort = AUTO_PORT_MIN + ((nextAutoPort - AUTO_PORT_MIN + 1) % AUTO_PORT_RANGE);
    if (autoAllocatedPorts.has(candidate)) {
      continue;
    }
    if (!(await canBindPort(candidate))) {
      continue;
    }
    autoAllocatedPorts.add(candidate);
    return candidate;
  }

  throw new Error("Failed to allocate a local Jazz server port.");
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
  const port = options.port ?? (await allocateAutoPort());
  const ownsPort = options.port === undefined;
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
    if (ownsPort) {
      autoAllocatedPorts.delete(port);
    }
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
        if (ownsPort) {
          autoAllocatedPorts.delete(port);
        }
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
