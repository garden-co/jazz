import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { boundApp } from "../typed-app.js";

interface Declared {
  schema: {
    current: string;
    encryption?: { versions?: Record<string, object> };
    schemas?: { hash: string; schema: { tables?: Record<string, unknown> } }[];
  };
}

/**
 * Whether the app is an encrypted app: its current version declares
 * encryption, or keeps Jazz's system tables without declaring an encrypted
 * column (once added, the tables stay).
 */
export function declaresEncryption(app: SchemaSourceInput): boolean {
  const bound = boundApp(resolveSchemaSource(app)) as Declared | undefined;
  if (!bound) return false;
  const current = bound.schema.encryption?.versions?.[bound.schema.current];
  if (current !== undefined && Object.keys(current).length > 0) return true;
  const version = bound.schema.schemas?.find((entry) => entry.hash === bound.schema.current);
  return Object.keys(version?.schema.tables ?? {}).some((name) => name.startsWith("__e2ee_"));
}

/**
 * What a backend opens with for an app that declares encryption: the
 * bindings' compiled app, from which it builds keyless hooks. `undefined`
 * for an app that declares none.
 */
export function keylessBackend(
  app: SchemaSourceInput,
  appId: string,
  environment: string,
): { bindings: string; appId: string; environment: string } | undefined {
  if (!declaresEncryption(app)) return undefined;
  const bound = boundApp(resolveSchemaSource(app))!;
  return {
    bindings: JSON.stringify({ schema: bound.schema, relations: bound.relations }),
    appId,
    environment,
  };
}
