/**
 * Publishing an app to a Jazz server. Jazz's Rust loader builds the
 * deployment from the app's bindings and `jazz.permissions.json` (jazz-napi
 * `loadBackendApp(...).deployment`): the schema versions and migrations the
 * bindings ship, and the compiled permissions. This only sends it, leaving
 * out the versions and migrations the server already stores.
 */
import type { SchemaSourceInput } from "../schema-source.js";
import {
  publishDeployment,
  type DeploymentBody,
  type DeploymentResponse,
} from "./catalogue-api.js";
import { shortSchemaHash } from "./schema-utils.js";
import { fetchMigrationGraph } from "./migration-graph.js";
import { isPermissionsFile, loadBackendAppFor, type PermissionsFile } from "./codegen-napi.js";

export { shortSchemaHash };
export type { SchemaSourceInput };

export interface CatalogueServerOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export interface DeploySchemaResult {
  hash: string;
  status: "published" | "already-stored";
}

export interface DeployResult extends DeploymentResponse {
  schema: DeploySchemaResult;
  warnings: string[];
}

/** What {@link deploy} publishes: the app from its bindings, and its `jazz.permissions.json`. */
export interface DeployOptions extends CatalogueServerOptions {
  /** The generated `app` (or `testApp`'s). */
  app: SchemaSourceInput;
  /** The app's `jazz.permissions.json`, imported as JSON or written inline. */
  permissions: PermissionsFile;
  /**
   * Confirms switching tables of the active schema to latest-only history,
   * which drops their past versions for good.
   */
  allowDroppingHistory?: boolean;
  /**
   * The decrypt conversions this deploy confirms, as `table.column`
   * (`["notes.body"]`). A migration that decrypts a column puts its
   * plaintext on the server and every replica, so the server refuses it
   * until the deploy names the column.
   */
  confirmDecrypt?: string[];
}

/** A deployment Jazz's Rust loader built, with the warnings it raised. */
export interface LoadedDeployment {
  deployment: unknown;
  warnings: string[];
}

/**
 * Publish a deployment the Rust loader built (`loadBackendApp`), without the
 * schema versions and migrations the server already stores. Isomorphic: a
 * browser test publishes what a Node command loaded.
 */
export async function publishLoadedDeployment(
  options: CatalogueServerOptions & {
    allowDroppingHistory?: boolean;
    confirmDecrypt?: string[];
  },
  loaded: LoadedDeployment,
): Promise<DeployResult> {
  const deployment = loaded.deployment as DeploymentBody;
  const graph = await fetchMigrationGraph(options);
  const storedSchemas = new Set(graph.schemas);
  const storedMigrations = new Set(
    graph.migrations.map((edge) => `${edge.fromHash}:${edge.toHash}`),
  );
  const result = await publishDeployment(options.serverUrl, options, {
    ...deployment,
    schemas: deployment.schemas.filter(({ hash }) => !storedSchemas.has(hash)),
    migrations: deployment.migrations.filter(
      (edge) => !storedMigrations.has(`${edge.fromHash}:${edge.toHash}`),
    ),
    allowDroppingHistory: options.allowDroppingHistory === true,
    // Sent only when the deployer confirms a decrypt.
    ...(options.confirmDecrypt?.length ? { confirmDecrypt: options.confirmDecrypt } : {}),
  });
  return {
    ...result,
    schema: {
      hash: deployment.targetSchemaHash,
      status: result.published.schemas.includes(deployment.targetSchemaHash)
        ? "published"
        : "already-stored",
    },
    warnings: [...loaded.warnings, ...permissionCoverageWarnings(deployment)],
  };
}

/**
 * A warning per table of the target schema that the deployment's
 * permissions give no policy at all: users can't read, insert, update or
 * delete its rows. That is fine for a table only the backend uses, which an
 * explicit `"false"` policy says, silencing the warning.
 */
export function permissionCoverageWarnings(deployment: DeploymentBody): string[] {
  const target = deployment.schemas.find(({ hash }) => hash === deployment.targetSchemaHash);
  const tables = (target?.schema as { tables?: Record<string, unknown> } | undefined)?.tables;
  if (!tables) return [];
  const permissions = (deployment.permissions ?? {}) as Record<
    string,
    Record<string, { using?: unknown; with_check?: unknown } | null> | undefined
  >;
  return Object.keys(tables)
    .filter((table) => {
      // Jazz's system tables get their policies from the encryption declarations.
      if (table.startsWith("__e2ee_")) return false;
      const operations = Object.values(permissions[table] ?? {});
      return !operations.some((policy) => policy?.using != null || policy?.with_check != null);
    })
    .map(
      (table) =>
        `table "${table}" has no policies in jazz.permissions.json, so users can't read, insert, update or delete its rows. ` +
        "Your backend isn't checked when it acts as the system, so if only it uses this table, that's fine: " +
        `write \`"${table}": { "read": "false" }\` to say so and silence this warning. ` +
        "Don't add allow rules just to silence it.",
    );
}

/**
 * Publish an app as its files declare it: every schema version and
 * migration its bindings ship, and its `jazz.permissions.json`, checked and
 * compiled by Jazz's Rust loader. This is what `createJazz` publishes on
 * start, so anything else that serves the app (a test server) publishes the
 * same. Node only.
 */
export async function deploy(options: DeployOptions): Promise<DeployResult> {
  if (!isPermissionsFile(options.permissions)) {
    throw new Error(
      'deploy requires the app\'s jazz.permissions.json (`{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json", "tables": {} }` denies all access).',
    );
  }
  return publishLoadedDeployment(
    options,
    await loadBackendAppFor(options.app, options.permissions),
  );
}

/** {@link deploy}, by the name `createJazz` and the test servers use. */
export const deployApp = deploy;
