import { createServer as createNetServer } from "node:net";
import { DevServer } from "jazz-napi";
import { loadCompiledSchema } from "../schema-loader.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../runtime/schema-fetch.js";

const DEFAULT_APP_ID = "00000000-0000-0000-0000-000000000001";
const AUTO_PORT_MIN = 20_000;
const AUTO_PORT_RANGE = 20_000;

const autoAllocatedPorts = new Set<number>();

let nextAutoPort = AUTO_PORT_MIN + Math.floor(Math.random() * AUTO_PORT_RANGE);

export interface StartLocalJazzServerOptions {
  appId?: string;
  port?: number;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  allowAnonymous?: boolean;
  allowDemo?: boolean;
  backendSecret?: string;
  adminSecret?: string;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
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

export async function startLocalJazzServer(
  options: StartLocalJazzServerOptions = {},
): Promise<LocalJazzServerHandle> {
  const appId = options.appId ?? DEFAULT_APP_ID;
  const port = options.port ?? (await allocateAutoPort());

  const server = await DevServer.start({
    appId,
    port,
    dataDir: options.dataDir,
    inMemory: options.inMemory,
    jwksUrl: options.jwksUrl,
    allowAnonymous: options.allowAnonymous,
    allowDemo: options.allowDemo,
    backendSecret: options.backendSecret,
    adminSecret: options.adminSecret,
    catalogueAuthority: options.catalogueAuthority,
    catalogueAuthorityUrl: options.catalogueAuthorityUrl,
    catalogueAuthorityAdminSecret: options.catalogueAuthorityAdminSecret,
  });

  if (options.enableLogs === true) {
    console.log(`[jazz-server] started on ${server.url}`);
  }

  return {
    appId: server.appId,
    port: server.port,
    url: server.url,
    dataDir: server.dataDir,
    adminSecret: server.adminSecret ?? undefined,
    backendSecret: server.backendSecret ?? undefined,
    stop: () => server.stop(),
  };
}

export async function pushSchemaCatalogue(
  options: PushSchemaCatalogueOptions,
): Promise<{ hash: string }> {
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

  return { hash: result.hash };
}
