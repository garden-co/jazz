import {
  startTestJazz,
  type TestJazz,
  type DeployOptions,
  type LocalJazzServerHandle,
} from "../../testing/index.js";
import type { SchemaSourceInput } from "../../schema-source.js";
import type { CompiledPermissionsMap } from "../../schema-permissions.js";

export type E2eeTestCore = LocalJazzServerHandle & Pick<TestJazz, "redeploy">;

/** Exercise the normal app backend; a core does not admit end-user sockets. */
export async function frontE2eeCore(
  core: LocalJazzServerHandle,
  app: { schema: SchemaSourceInput; permissions: CompiledPermissionsMap },
): Promise<E2eeTestCore> {
  const front = await startTestJazz({ core, app: app.schema, permissions: app.permissions });
  return {
    ...core,
    url: front.url,
    redeploy: (next) => front.redeploy(next),
    async stop() {
      try {
        await front.stop();
      } finally {
        await core.stop();
      }
    },
  };
}

/** Publish then restart the real app backend on the same user origins. */
export function redeployE2eeApp(
  front: E2eeTestCore,
  options: DeployOptions & { schema: SchemaSourceInput; permissions: CompiledPermissionsMap },
) {
  return front.redeploy({
    app: options.schema,
    permissions: options.permissions,
    migration: options.migration,
    migrations: options.migrations,
    schemas: options.schemas,
  });
}
