/**
 * The app's schema versions and the migrations between them, as `jazz-tools`
 * writes them to `migrations/bundle.json` (the `schemas` and `migrations` a
 * deployment publishes). A client opened with them stores every column where
 * the server does, even for a version it opens before it ever syncs.
 */
export interface ClientMigrations {
  schemas: Array<{ hash: string; schema: unknown }>;
  migrations: Array<{ fromHash: string; toHash: string; forward: unknown[] }>;
}
