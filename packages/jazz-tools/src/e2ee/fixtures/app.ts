import { startTestJazz, type LocalJazzServerHandle } from "../../testing/index.js";
import type { SchemaSourceInput } from "../../schema-source.js";
import type { CompiledPermissionsMap } from "../../schema-permissions.js";

/** Exercise the normal app backend; a core does not admit end-user sockets. */
export async function frontE2eeCore(
  core: LocalJazzServerHandle,
  app: { schema: SchemaSourceInput; permissions: CompiledPermissionsMap },
): Promise<LocalJazzServerHandle> {
  const front = await startTestJazz({ core, app: app.schema, permissions: app.permissions });
  return {
    ...core,
    url: front.url,
    async stop() {
      try {
        await front.stop();
      } finally {
        await core.stop();
      }
    },
  };
}
