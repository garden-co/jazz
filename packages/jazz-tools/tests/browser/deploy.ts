import { jazzDeployBrowserCommands } from "./browser-commands.js";
import type { DeployResult } from "../../src/dev/catalogue.js";
import type { CompiledPermissionsMap } from "../../src/schema-permissions.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../../src/schema-source.js";

/**
 * `deploy` from a browser test: publishes `schema` with `permissions` from
 * the Node side of the runner (`jazzDeploy`), since deploying hashes schemas
 * with Jazz's Rust hash through jazz-napi.
 */
export async function deploy(options: {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schema: SchemaSourceInput;
  permissions: CompiledPermissionsMap;
}): Promise<DeployResult> {
  return jazzDeployBrowserCommands().jazzDeploy({
    appId: options.appId,
    serverUrl: options.serverUrl,
    adminSecret: options.adminSecret,
    schema: resolveSchemaSource(options.schema),
    permissions: options.permissions,
  });
}
