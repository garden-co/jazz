import type { WasmSchema } from "../drivers/types.js";

/**
 * The app's schema versions and the migrations between them (the `schemas`
 * and `migrations` a deployment publishes). `jazz-tools` generates them in
 * `migrations/index.ts` beside schema.ts on every validate and deploy, even
 * while the app has a single version, and the app passes them to
 * `defineApp`:
 *
 * ```ts
 * import { migrations } from "./migrations";
 * export const app = s.defineApp(schema, { migrations });
 * ```
 *
 * A client using the app stores every column where the server does, even
 * for a version it opens before it ever syncs, and refuses a version they
 * lack.
 */
export interface ClientMigrations {
  schemas: Array<{ hash: string; schema: unknown }>;
  migrations: Array<{ fromHash: string; toHash: string; forward: unknown[] }>;
}

const shipped = new WeakMap<WasmSchema, ClientMigrations>();

/** Record that the app defining `schema` ships `migrations`. */
export function shipMigrations(
  schema: WasmSchema,
  migrations: ClientMigrations | "none" | undefined,
): void {
  if (migrations === undefined) {
    throw new Error(
      'defineApp needs the app\'s migrations: `s.defineApp(schema, { migrations })` with `import { migrations } from "./migrations"`, or `{ migrations: "none" }` for an app that is never deployed',
    );
  }
  if (migrations !== "none") shipped.set(schema, migrations);
}

/** The migrations the app defining `schema` ships, if it ships any. */
export function shippedMigrations(schema: WasmSchema): ClientMigrations | undefined {
  return shipped.get(schema);
}
