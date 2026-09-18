import type { Db, QueryBuilder } from "../runtime/db.js";
import type { ColumnType } from "../drivers/types.js";
import { normalizeBuiltQuery } from "../runtime/query-builder-shape.js";
import { resolveSelectedColumns } from "../runtime/select-projection.js";
import { toWriteRecord } from "../runtime/value-converter.js";
import { toTimestampMs } from "../runtime/query-adapter.js";
import { TypedTableQueryBuilder } from "../typed-app.js";
import { encryptedSchemas, equalityIndexColumn } from "./encrypted-schema.js";
import { equalityToken, equalityValue } from "./equality-data.js";
import { decryptedIndexBytes } from "./cell-data.js";
import { withSpaceKeys } from "./lifecycle.js";
import { E2eeDataError } from "./data-error.js";
import type { SpaceRoot } from "./spaces.js";

/** Restore typed literals after query JSON serialisation, using the logical schema. */
function restoreQueryLiteral(value: unknown, type: ColumnType): unknown {
  if (type.type === "BigInt" && typeof value === "string") return BigInt(value);
  if (type.type === "Timestamp" && typeof value === "string") return toTimestampMs(value);
  if (type.type === "Array" && Array.isArray(value))
    return value.map((item) => restoreQueryLiteral(item, type.element));
  if (
    type.type === "EnumPayload" &&
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value)
  ) {
    const input = value as Record<string, unknown>;
    const entry = type.cases.find((candidate) => candidate.name === input.type);
    if (entry)
      return {
        ...input,
        ...Object.fromEntries(
          entry.fields
            .filter((field) => Object.hasOwn(input, field.name))
            .map((field) => [
              field.name,
              restoreQueryLiteral(input[field.name], field.column_type),
            ]),
        ),
      };
  }
  return value;
}

/** A complete case match is whole-value equality; a partial match is not. */
function wholeEnumMatch(value: unknown, type: ColumnType): unknown {
  if (type.type === "EnumPayload" && value && typeof value === "object") {
    const match = value as { type?: unknown; where?: unknown };
    const entry = type.cases.find((candidate) => candidate.name === match.type);
    const fields = match.where === undefined ? {} : match.where;
    if (entry && fields && typeof fields === "object" && !Array.isArray(fields)) {
      const names = Object.keys(fields);
      if (
        names.length === entry.fields.length &&
        entry.fields.every((field) => Object.hasOwn(fields, field.name))
      ) {
        return { type: entry.name, ...fields };
      }
    }
  }
  throw new Error("Unsupported encrypted query: enum match must specify every payload field");
}

/** Prepare physical candidates, then verify and paginate logical matches. */
export async function prepareEqualityQuery(
  db: Db,
  query: QueryBuilder<unknown>,
  json: string,
  transaction?: {
    ready: () => Promise<void> | undefined;
    initialSpaceKeys: ReadonlyMap<string, { secret: Uint8Array; root: SpaceRoot }>;
  },
) {
  const metadata = encryptedSchemas.get(query._schema);
  if (!metadata) return undefined;
  const built = normalizeBuiltQuery(JSON.parse(json));
  const declaration = metadata.tables.get(built.table);
  if (!declaration) return undefined;
  const predicates = built.conditions.filter((condition) =>
    declaration.columns.includes(condition.column),
  );
  if (!predicates.length) return undefined;
  if (built.hops.length || built.gather || built.union || Object.keys(built.partialSelect).length) {
    throw new Error("Unsupported encrypted query plan");
  }
  for (const predicate of predicates) {
    if (
      !declaration.indexes?.[predicate.column] ||
      (predicate.op !== "eq" && predicate.op !== "match")
    )
      throw new Error(
        `Unsupported encrypted query: "${predicate.column}" requires an equality index`,
      );
  }
  const scopeCondition = built.conditions.find(
    (condition) => condition.column === declaration.space && condition.op === "eq",
  );
  if (typeof scopeCondition?.value !== "string")
    throw new Error("Encrypted equality queries require a plaintext space constraint");
  const table = new TypedTableQueryBuilder(built.table, query._schema);
  const scope = new TypedTableQueryBuilder(declaration.scope, query._schema);
  const values = predicates.map((predicate) => {
    const column = metadata.logical[built.table]!.columns.find(
      (item) => item.name === predicate.column,
    )!;
    const value =
      predicate.op === "match"
        ? wholeEnumMatch(predicate.value, column.column_type)
        : predicate.value;
    return toWriteRecord(
      { [predicate.column]: restoreQueryLiteral(value, column.column_type) },
      metadata.logical,
      built.table,
    )[predicate.column]!;
  });
  const tokens: number[][][] = predicates.map(() => []);
  const epochs = new Set<string>();
  await transaction?.ready();
  const initial = transaction?.initialSpaceKeys.size
    ? transaction.initialSpaceKeys.get(`${await db.tableIdentity(scope)}:${scopeCondition.value}`)
    : undefined;
  const collect = async (secret: Uint8Array, root: Readonly<SpaceRoot>) => {
    epochs.add(root.epochId);
    for (const [i, predicate] of predicates.entries())
      tokens[i]!.push([
        ...(await equalityToken(db, table, predicate.column, values[i]!, secret, root)),
      ]);
  };
  if (initial) await collect(initial.secret, initial.root);
  else await withSpaceKeys(db, scope, scopeCondition.value, collect, true);
  if (tokens.some((values) => !values.length)) throw new E2eeDataError("key-unavailable");
  const wanted = predicates.map((predicate, i) =>
    equalityValue(table, predicate.column, values[i]!),
  );
  const physical = {
    ...JSON.parse(json),
    conditions: built.conditions.map((condition) => {
      const i = predicates.indexOf(condition);
      return i < 0
        ? condition
        : { column: equalityIndexColumn(condition.column), op: "in", value: tokens[i] };
    }),
    select: [],
    limit: undefined,
    offset: undefined,
  };
  const selection = built.select.length
    ? new Set([
        "id",
        ...resolveSelectedColumns(built.table, query._schema, built.select),
        ...Object.keys(built.includes),
      ])
    : undefined;
  return {
    json: JSON.stringify(physical),
    space: { scope: declaration.scope, identifier: scopeCondition.value },
    async isCurrent() {
      // This key belongs to the atomic transaction, not published history yet.
      if (initial) return true;
      const current = new Set<string>();
      // Re-read accepted history after the candidate read. An epoch absent from
      // the prepared tokens may contain matches the physical predicate omitted.
      await withSpaceKeys(
        db,
        scope,
        scopeCondition.value as string,
        async (_secret, root) => {
          current.add(root.epochId);
        },
        true,
      );
      return current.size === epochs.size && [...current].every((epoch) => epochs.has(epoch));
    },
    dispose() {
      for (const bytes of wanted) bytes.fill(0);
    },
    verify(rows: Record<string, unknown>[]) {
      const verified = rows.filter((row) =>
        predicates.every((predicate, i) => {
          const bytes = decryptedIndexBytes.get(row)?.get(predicate.column);
          if (!bytes) throw new E2eeDataError("invalid-ciphertext");
          return (
            bytes.length === wanted[i]!.length && bytes.every((byte, j) => byte === wanted[i]![j])
          );
        }),
      );
      const start = built.offset ?? 0;
      return verified
        .slice(start, built.limit === undefined ? undefined : start + built.limit)
        .map((row) =>
          selection
            ? Object.fromEntries(Object.entries(row).filter(([name]) => selection.has(name)))
            : row,
        );
    },
  };
}
