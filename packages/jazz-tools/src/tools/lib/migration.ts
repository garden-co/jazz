import { TypeSym, type CoValue } from "../internal.js";

export function applyCoValueMigrations(instance: CoValue) {
  const node = instance.$jazz.raw.core.node;

  // @ts-expect-error _migratedCoValues is a custom expando property
  const migratedCoValues = (node._migratedCoValues ??= new Set<string>());

  if (
    "migrate" in instance &&
    typeof instance.migrate === "function" &&
    instance[TypeSym] !== "Account" &&
    !migratedCoValues.has(instance.$jazz.id)
  ) {
    // We flag this before the migration to avoid that internal loads trigger the migration again
    migratedCoValues.add(instance.$jazz.id);

    const result = instance.migrate?.(instance);
    if (result && "then" in result) {
      throw new Error("Migration function cannot be async");
    }
  }
}
