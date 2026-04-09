import { DevServer } from "jazz-napi";
import { loadCompiledSchema } from "../schema-loader.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../runtime/schema-fetch.js";

const DEFAULT_APP_ID = "00000000-0000-0000-0000-000000000001";
const DEFAULT_PORT = 1625;

export interface StartLocalJazzServerOptions {
  appId?: string;
  port?: number;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  allowSelfSigned?: boolean;
  selfSignedAudience?: string;
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

export async function startLocalJazzServer(
  options: StartLocalJazzServerOptions = {},
): Promise<LocalJazzServerHandle> {
  const appId = options.appId ?? DEFAULT_APP_ID;
  const port = options.port ?? DEFAULT_PORT;

  const server = await DevServer.start({
    appId,
    port,
    dataDir: options.dataDir,
    inMemory: options.inMemory,
    jwksUrl: options.jwksUrl,
    allowSelfSigned: options.allowSelfSigned,
    selfSignedAudience: options.selfSignedAudience,
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
