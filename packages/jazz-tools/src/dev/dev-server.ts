import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "../schema-permissions.js";
import { randomUUID } from "node:crypto";
import { isIP } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JazzServer } from "jazz-napi";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { shippedArtifact, shipsMigrations } from "../runtime/app-artifact.js";
import { deployApp } from "./catalogue.js";

export { deploy, type DeployOptions } from "./catalogue.js";

const DEFAULT_APP_ID = "00000000-0000-0000-0000-000000000001";
function validateServerHost(host: string | undefined): string | undefined {
  if (host === undefined) return undefined;
  if (isIP(host) === 0) {
    throw new Error(`Invalid Jazz server host "${host}". Expected a concrete IP address.`);
  }
  if (host === "0.0.0.0" || host === "::") {
    throw new Error(`Invalid Jazz server host "${host}". Wildcard addresses are not supported.`);
  }
  return host;
}

interface LocalJazzServerOptions {
  /** Concrete IP address used for both listener binding and the advertised URL. */
  host?: string;
  appId?: string;
  port?: number;
  dataDir?: string;
  inMemory?: boolean;
  backendSecret?: string;
  adminSecret?: string;
  telemetryCollectorUrl?: string;
  /** Live WebSocket connections one signed-in user may hold (default 128). */
  maxConnectionsPerUser?: number;
  /** Trust only this OIDC workload instead of the shared secrets. */
  trustedWorkload?: { issuer: string; audience: string; subject: string };
  enableLogs?: boolean;
}

export type StartLocalJazzServerOptions = LocalJazzServerOptions &
  (
    | { schema: SchemaSourceInput; permissions: CompiledPermissionsMap }
    | { schema?: undefined; permissions?: undefined }
  );

export interface LocalJazzServerHandle {
  appId: string;
  port: number;
  url: string;
  dataDir: string;
  /** Empty for a server that trusts a workload instead. */
  adminSecret: string;
  /** Empty for a server that trusts a workload instead. */
  backendSecret: string;
  stop: () => Promise<void>;
}

async function createOwnedDataDir(): Promise<string> {
  return await mkdtemp(join(tmpdir(), "jazz-dev-server-"));
}

/**
 * Start a local Jazz sync server.
 *
 * To initialize an app, pass its schema and permissions. Omit both to deploy later.
 * An app that ships migrations (`defineApp(schema, { migrations })`) is
 * published with all of them, as `createJazz` publishes it.
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
  if ((options as { upstreamUrl?: unknown }).upstreamUrl !== undefined) {
    throw new Error(
      "Server edges are no longer supported. Remove upstreamUrl and connect clients directly to Core.",
    );
  }
  if (options.schema !== undefined && options.permissions == null) {
    throw new Error(
      "startLocalJazzServer requires permissions when schema is provided. Pass {} to deny all access.",
    );
  }
  const host = validateServerHost(options.host);
  const declared =
    options.schema === undefined
      ? undefined
      : {
          source: options.schema,
          schema: resolveSchemaSource(options.schema),
          permissions: options.permissions,
        };
  // An app that ships migrations is published as createJazz publishes it
  // (deployApp): every schema version and lens it ships. Started with only
  // its current schema, the server would take that as the app's first
  // version, and refuse the app's own deployment and clients opening with
  // its lenses.
  const shipsHistory = declared !== undefined && shipsMigrations(shippedArtifact(declared.schema));
  if (shipsHistory && options.trustedWorkload !== undefined) {
    throw new Error(
      "startLocalJazzServer: an app that ships migrations is not supported with trustedWorkload yet; start without schema and deploy the app with its admin secret.",
    );
  }
  const schema =
    declared === undefined || shipsHistory
      ? undefined
      : encodeSchema(mergePermissionsIntoWasmSchema(declared.schema, declared.permissions));
  const appId = options.appId ?? DEFAULT_APP_ID;
  // Ask the server to bind port 0 when callers do not require a particular
  // address. Unlike probing a candidate port first, this is atomic and remains
  // safe across Vitest worker processes (and separate concurrent CI jobs).
  const port = options.port ?? 0;
  const ownsDataDir = options.inMemory !== true && options.dataDir === undefined;
  const dataDir = ownsDataDir ? await createOwnedDataDir() : options.dataDir;
  // A trusted workload replaces the shared secrets.
  const workload = options.trustedWorkload !== undefined;
  if (workload && (options.adminSecret !== undefined || options.backendSecret !== undefined)) {
    throw new Error("startLocalJazzServer: trustedWorkload replaces adminSecret and backendSecret");
  }
  const adminSecret = workload
    ? undefined
    : (options.adminSecret ?? `jazz-test-admin-${randomUUID().slice(0, 8)}`);
  const backendSecret = workload
    ? undefined
    : (options.backendSecret ?? `jazz-test-backend-${randomUUID().slice(0, 8)}`);

  let server;
  try {
    server = await JazzServer.start({
      appId,
      port,
      host,
      dataDir,
      inMemory: options.inMemory,
      backendSecret,
      adminSecret,
      telemetryCollectorUrl: options.telemetryCollectorUrl,
      maxConnectionsPerUser: options.maxConnectionsPerUser,
      schema: schema ? [...schema] : undefined,
      trustedWorkload: options.trustedWorkload,
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

  if (shipsHistory && declared) {
    try {
      await deployApp({
        appId: server.appId,
        serverUrl: server.url,
        adminSecret: server.adminSecret,
        app: declared.source,
        permissions: declared.permissions,
      });
    } catch (error) {
      await server.stop().catch(() => undefined);
      if (ownsDataDir && dataDir) {
        await rm(dataDir, { recursive: true, force: true }).catch(() => undefined);
      }
      throw error;
    }
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
