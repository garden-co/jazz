import type { DBTableIndex } from "better-auth/db";

const invalidIndexMetadata = (table: string, detail: string): Error =>
  new Error(`[Jazz Better Auth adapter] Invalid index metadata for table "${table}": ${detail}.`);

/**
 * Better Auth validates its schema before constructing an adapter, but plugins
 * are JavaScript at runtime. Validate the metadata again at the point where we
 * rely on it for application-level uniqueness so a malformed plugin produces a
 * useful adapter diagnostic instead of an incidental TypeError.
 */
export const readTableIndexes = (table: string, indexes: unknown): readonly DBTableIndex[] => {
  if (indexes === undefined) return [];
  if (!Array.isArray(indexes)) {
    throw invalidIndexMetadata(table, "expected indexes to be an array");
  }

  const validated: DBTableIndex[] = [];
  for (let position = 0; position < indexes.length; position++) {
    if (!Object.hasOwn(indexes, position)) {
      throw invalidIndexMetadata(table, `index ${position} must be present`);
    }

    const index = indexes[position];
    if (typeof index !== "object" || index === null) {
      throw invalidIndexMetadata(table, `index ${position} must be an object`);
    }

    const candidate = index as Partial<DBTableIndex>;
    if (!Array.isArray(candidate.fields) || candidate.fields.length === 0) {
      throw invalidIndexMetadata(
        table,
        `index ${position} must declare a non-empty string[] fields`,
      );
    }
    for (let fieldPosition = 0; fieldPosition < candidate.fields.length; fieldPosition++) {
      if (
        !Object.hasOwn(candidate.fields, fieldPosition) ||
        typeof candidate.fields[fieldPosition] !== "string"
      ) {
        throw invalidIndexMetadata(
          table,
          `index ${position} must declare a non-empty string[] fields`,
        );
      }
    }
    if (candidate.unique !== undefined && typeof candidate.unique !== "boolean") {
      throw invalidIndexMetadata(table, `index ${position} unique must be a boolean when present`);
    }

    validated.push(candidate as DBTableIndex);
  }

  return validated;
};
