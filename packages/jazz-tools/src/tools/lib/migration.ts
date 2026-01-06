import { TypeSym, type CoValue } from "../internal.js";

export function applyCoValueMigrations(
  instance: CoValue,
): void | Promise<void> {
  const node = instance.$jazz.raw.core.node;

  // @ts-expect-error _migratedCoValues is a custom expando property
  const migratedCoValues = (node._migratedCoValues ??= new Set<string>());

  if (
    "migrate" in instance &&
    typeof instance.migrate === "function" &&
    instance[TypeSym] !== "Account" &&
    // TODO shouldn't skip, because the covalue might not be migrated yet
    !migratedCoValues.has(instance.$jazz.id)
  ) {
    // We flag this before the migration to avoid that internal loads trigger the migration again
    migratedCoValues.add(instance.$jazz.id);

    return instance.migrate?.(instance);
  }
}
