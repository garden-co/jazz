/**
 * Publishing an app to a Jazz server: the schema versions and migrations its
 * bindings ship (`jazz.generated.ts`) and its permissions, as Jazz's Rust
 * loader compiled them from `jazz.permissions.json`.
 */
import { validateSchemaAndPermissions } from "./schema-validation.js";
import type { WasmSchema } from "../drivers/types.js";
import { shippedMigrations } from "../runtime/client-migrations.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { computeSchemaHash } from "../schema-hash.js";
import {
  publishDeployment,
  type DeploymentArtifacts,
  type DeploymentResponse,
  type PublishedTableLens,
} from "./catalogue-api.js";
import { shortSchemaHash } from "./schema-utils.js";
import { enginePermissions, type PermissionsFile } from "./codegen-napi.js";
import { fetchMigrationGraph, type MigrationGraph } from "./migration-graph.js";

export { computeSchemaHash, shortSchemaHash };
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

/** What {@link deploy} publishes: the app from its bindings, and its compiled permissions. */
export interface DeployOptions extends CatalogueServerOptions {
  /** The generated `app` (or its schema). */
  app: SchemaSourceInput;
  /** The app's `jazz.permissions.json` (or the engine's policies a load returned). */
  permissions: PermissionsFile | CompiledPermissionsMap;
  /**
   * Confirms switching tables of the active schema to latest-only history,
   * which drops their past versions for good.
   */
  allowDroppingHistory?: boolean;
}

/** A warning for each table without any policy: everything on it is denied. */
function missingPolicyWarnings(
  tables: readonly string[],
  permissions: CompiledPermissionsMap,
): string[] {
  return tables
    .filter((table) => {
      const policies = permissions[table];
      return !policies || Object.values(policies).every((policy) => policy == null);
    })
    .map(
      (table) =>
        `Table "${table}" has no policies in jazz.permissions.json, so every read and write of it is denied. ` +
        "Add the operations it allows under its table; don't add allow rules just to silence this.",
    );
}

/** Diff against the fetched graph and publish all missing artifacts in one request. */
export async function deployArtifacts(
  options: CatalogueServerOptions & { allowDroppingHistory?: boolean },
  graph: MigrationGraph,
  artifacts: DeploymentArtifacts,
): Promise<DeployResult> {
  const storedSchemas = new Set(graph.schemas);
  const storedMigrations = new Set(
    graph.migrations.map((edge) => `${edge.fromHash}:${edge.toHash}`),
  );
  const result = await publishDeployment(options.serverUrl, {
    ...options,
    ...artifacts,
    schemas: artifacts.schemas.filter(({ hash }) => !storedSchemas.has(hash)),
    migrations: artifacts.migrations.filter(
      (edge) => !storedMigrations.has(`${edge.fromHash}:${edge.toHash}`),
    ),
  });
  const target = artifacts.schemas.find(({ hash }) => hash === artifacts.targetSchemaHash)!;
  return {
    ...result,
    schema: {
      hash: artifacts.targetSchemaHash,
      status: result.published.schemas.includes(artifacts.targetSchemaHash)
        ? "published"
        : "already-stored",
    },
    warnings: missingPolicyWarnings(Object.keys(target.schema), artifacts.permissions),
  };
}

/**
 * Publish an app as its files declare it: every schema version and migration
 * its bindings ship, and `permissions`. This is what `createJazz` publishes on start, so anything
 * else that serves the app (a test server) must publish the same, or a client
 * opening the app with its lenses disagrees with the server about its history.
 */
export async function deployApp(options: DeployOptions): Promise<DeployResult> {
  const declared = resolveSchemaSource(options.app);
  const permissions = await enginePermissions(options.app, options.permissions);
  const schema = mergePermissionsIntoWasmSchema(declared, {});
  await validateSchemaAndPermissions(schema, permissions);
  const targetSchemaHash = await computeSchemaHash(schema);
  const shipped = shippedMigrations(declared);
  const schemas = new Map<string, WasmSchema>();
  for (const version of shipped?.schemas ?? []) {
    schemas.set(version.hash, (version.schema as { tables: WasmSchema }).tables);
  }
  schemas.set(targetSchemaHash, schema);
  const graph = await fetchMigrationGraph(options);
  return deployArtifacts(options, graph, {
    targetSchemaHash,
    schemas: [...schemas].map(([hash, schema]) => ({ hash, schema })),
    migrations: (shipped?.migrations ?? []).map((edge) => ({
      fromHash: edge.fromHash,
      toHash: edge.toHash,
      forward: edge.forward as PublishedTableLens[],
    })),
    permissions,
  });
}

/** Publish an app and its permissions (what `createJazz` publishes when it starts). */
export const deploy = deployApp;
