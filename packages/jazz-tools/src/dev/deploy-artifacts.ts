import { collectMissingExplicitPolicyDiagnostics } from "../schema-permissions.js";
import { publishDeployment, type DeploymentArtifacts } from "./catalogue-api.js";
import type { CatalogueServerOptions, DeployResult } from "./catalogue.js";
import type { MigrationGraph } from "./migration-graph.js";

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
    warnings: collectMissingExplicitPolicyDiagnostics(
      Object.keys(target.schema),
      artifacts.permissions,
    ).map((diagnostic) => diagnostic.message),
  };
}
