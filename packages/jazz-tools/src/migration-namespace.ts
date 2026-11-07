import { migrationOperations } from "./dsl.js";
import { defineMigration, renameTableFrom } from "./migrations.js";

/** Bidirectional migration builders shared by every public binding. */
export const migration: typeof migrationOperations & {
  defineMigration: typeof defineMigration;
  renameTableFrom: typeof renameTableFrom;
} = {
  ...migrationOperations,
  defineMigration,
  renameTableFrom,
};
