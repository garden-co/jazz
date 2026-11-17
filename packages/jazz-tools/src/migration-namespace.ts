import { migrationOperations } from "./dsl.js";
import { defineMigration, renameTableFrom } from "./migrations.js";

/**
 * Bidirectional migration builders (`migrations/*.ts`), shared by every
 * public binding: `m.defineMigration`, `m.add.*`, `m.drop.*`,
 * `m.renameFrom`, `m.renameTableFrom`. `jazz-tools generate` reads them.
 */
export const migration: typeof migrationOperations & {
  defineMigration: typeof defineMigration;
  renameTableFrom: typeof renameTableFrom;
} = {
  ...migrationOperations,
  defineMigration,
  renameTableFrom,
};
