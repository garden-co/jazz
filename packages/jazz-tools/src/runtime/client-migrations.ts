import type { WasmSchema } from "../drivers/types.js";

/**
 * The app's schema versions and the migrations between them (the `schemas`
 * and `migrations` a deployment publishes), as Jazz's Rust loader lowers
 * them from `jazz.app.json`. `jazz.generated.ts` embeds them and `bindApp`
 * ships them with the app's schema.
 *
 * A client using the app stores every column where the server does, even
 * for a version it opens before it ever syncs, and refuses a version they
 * lack.
 */
export interface ClientMigrations {
  schemas: Array<{ hash: string; schema: unknown }>;
  migrations: Array<{ fromHash: string; toHash: string; forward: unknown[] }>;
}

/**
 * Where the app's schema carries the migrations it ships: a property on the
 * schema object itself, under a registered symbol, so that every copy of
 * jazz-tools in a process reads what `bindApp` recorded. A bundler can load
 * the module defining the app and the one opening the store as separate
 * copies (Next's server bundles do); state private to this module would then
 * leave the store opening the app without its migrations, as though its
 * current schema were its first. Not enumerable: the schema's JSON, and so its
 * hash, stay as they are.
 */
const SHIPPED = Symbol.for("jazz-tools.shippedMigrations");

/** Record that the app defining `schema` ships `migrations`. */
export function shipMigrations(schema: WasmSchema, migrations: ClientMigrations): void {
  Object.defineProperty(schema, SHIPPED, {
    value: migrations,
    enumerable: false,
    configurable: true,
    writable: false,
  });
}

/** The migrations the app defining `schema` ships, if it ships any. */
export function shippedMigrations(schema: WasmSchema): ClientMigrations | undefined {
  return (schema as { [SHIPPED]?: ClientMigrations })[SHIPPED];
}
