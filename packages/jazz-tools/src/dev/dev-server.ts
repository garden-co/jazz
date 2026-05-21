import { createServer as createNetServer } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DevServer } from "jazz-napi";
import { createMigration } from "../cli.js";
import { loadCompiledSchema } from "../schema-loader.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../runtime/schema-fetch.js";

const DEFAULT_APP_ID = "00000000-0000-0000-0000-000000000001";
const AUTO_PORT_MIN = 20_000;
const AUTO_PORT_RANGE = 20_000;
export const DEV_SERVER_MIGRATION_CREATE_PATH = "/_jazz/migrations/create";

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
  peerSecret?: string;
  allowLocalFirstAuth?: boolean;
  telemetryCollectorUrl?: string;
  enableLogs?: boolean;
}

export interface LocalJazzServerHandle {
  appId: string;
  port: number;
  url: string;
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
  enableLogs?: boolean;
}

export interface DevServerMigrationCreateOptions {
  serverUrl: string;
  appId: string;
  adminSecret: string;
  schemaDir: string;
}

interface DevServerMigrationCreateBody {
  fromHash?: unknown;
  toHash?: unknown;
  name?: unknown;
}

class DevServerMigrationCreateError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
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

export async function startLocalJazzServer(
  options: StartLocalJazzServerOptions = {},
): Promise<LocalJazzServerHandle> {
  const appId = options.appId ?? DEFAULT_APP_ID;
  const port = options.port ?? (await allocateAutoPort());
  const ownsPort = options.port === undefined;
  const ownsDataDir = options.inMemory !== true && options.dataDir === undefined;
  const dataDir = ownsDataDir ? await createOwnedDataDir() : options.dataDir;

  let server;
  try {
    server = await DevServer.start({
      appId,
      port,
      dataDir,
      inMemory: options.inMemory,
      jwksUrl: options.jwksUrl,
      backendSecret: options.backendSecret,
      adminSecret: options.adminSecret,
      upstreamUrl: options.upstreamUrl,
      peerSecret: options.peerSecret,
      allowLocalFirstAuth: options.allowLocalFirstAuth,
      telemetryCollectorUrl: options.telemetryCollectorUrl,
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
    adminSecret: server.adminSecret ?? undefined,
    backendSecret: server.backendSecret ?? undefined,
    stop,
  };
}

export async function pushSchemaCatalogue(
  options: PushSchemaCatalogueOptions,
): Promise<{ hash: string }> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  const result = await publishStoredSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
  });

  if (compiled.permissions) {
    const { head } = await fetchPermissionsHead(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
    });
    await publishStoredPermissions(options.serverUrl, {
      appId: options.appId,
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

  return { hash: result.hash };
}

async function readRequestBody(req: IncomingMessage): Promise<string> {
  let body = "";

  for await (const chunk of req) {
    body += typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
    if (body.length > 16_384) {
      throw new DevServerMigrationCreateError(413, "Request body is too large.");
    }
  }

  return body;
}

function parseMigrationCreateBody(rawBody: string): {
  fromHash?: string;
  toHash?: string;
  name?: string;
} {
  let body: DevServerMigrationCreateBody;
  try {
    body = JSON.parse(rawBody) as DevServerMigrationCreateBody;
  } catch {
    throw new DevServerMigrationCreateError(400, "Request body must be valid JSON.");
  }

  if (
    body.fromHash !== undefined &&
    (typeof body.fromHash !== "string" || body.fromHash.trim().length === 0)
  ) {
    throw new DevServerMigrationCreateError(400, "fromHash must be a non-empty string.");
  }
  if (
    body.toHash !== undefined &&
    (typeof body.toHash !== "string" || body.toHash.trim().length === 0)
  ) {
    throw new DevServerMigrationCreateError(400, "toHash must be a non-empty string.");
  }
  if ((body.fromHash === undefined) !== (body.toHash === undefined)) {
    throw new DevServerMigrationCreateError(
      400,
      "Provide both fromHash and toHash, or omit both to infer them.",
    );
  }
  if (body.name !== undefined && typeof body.name !== "string") {
    throw new DevServerMigrationCreateError(400, "name must be a string when provided.");
  }

  return {
    fromHash: body.fromHash?.trim(),
    toHash: body.toHash?.trim(),
    name: body.name?.trim() || undefined,
  };
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
}

export function createDevServerMigrationCreateHandler(options: DevServerMigrationCreateOptions) {
  return async (
    req: IncomingMessage,
    res: ServerResponse,
    next: (error?: unknown) => void,
  ): Promise<void> => {
    if (req.method !== "POST") {
      sendJson(res, 405, { error: "Use POST to create a migration." });
      return;
    }

    try {
      const request = parseMigrationCreateBody(await readRequestBody(req));
      let fromHash = request.fromHash;
      const toHash = request.toHash;

      if (!fromHash && !toHash) {
        const { head } = await fetchPermissionsHead(options.serverUrl, {
          appId: options.appId,
          adminSecret: options.adminSecret,
        });
        if (!head) {
          throw new DevServerMigrationCreateError(
            409,
            "Cannot infer migration source because the server has no permissions head.",
          );
        }
        fromHash = head.schemaHash;
      }

      const filePath = await createMigration({
        appId: options.appId,
        serverUrl: options.serverUrl,
        adminSecret: options.adminSecret,
        schemaDir: options.schemaDir,
        migrationsDir: join(options.schemaDir, "migrations"),
        fromHash,
        toHash,
        name: request.name,
      });

      sendJson(res, 200, { filePath });
    } catch (error) {
      if (error instanceof DevServerMigrationCreateError) {
        sendJson(res, error.status, { error: error.message });
        return;
      }

      if (error instanceof Error) {
        sendJson(res, 500, { error: error.message });
        return;
      }

      next(error);
    }
  };
}

export function devServerMigrationRunnerScript(): string {
  return `<script>
globalThis.runJazzMigrations = async function runJazzMigrations() {
  const response = await fetch(${JSON.stringify(DEV_SERVER_MIGRATION_CREATE_PATH)}, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
  const body = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = body && typeof body.error === "string" ? \`: \${body.error}\` : "";
    throw new Error(\`Jazz migration creation failed (\${response.status} \${response.statusText})\${detail}\`);
  }
  if (body && typeof body.filePath === "string") {
    console.log(\`[jazz] Migration created: \${body.filePath}\`);
  } else {
    console.log("[jazz] No migration file created.", body);
  }
  return body;
};
</script>`;
}
