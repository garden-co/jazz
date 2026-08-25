import {
  createAdapterFactory,
  DBAdapterDebugLogOption,
  type CleanedWhere,
} from "better-auth/adapters";
import { PersistedWriteRejectedError } from "../runtime/client.js";
import type { Db, QueryBuilder, TransactionScope } from "../runtime/db.js";
import type { BackendSchemaInput } from "../backend/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { createJazzSchemaSourceFile } from "./schema.js";
import { readTableIndexes } from "./indexes.js";
import type { JazzBuiltCondition, JazzRowRecord, JazzSortBy } from "./types.js";
import {
  filterListByWhere,
  paginateList,
  sortListByField,
  assertNativeJoinsDisabled,
  isQuerySupported,
  createQueryBuilder,
} from "./utils.js";

interface JazzAdapterConfig {
  debugLogs?: DBAdapterDebugLogOption;
  usePlural?: boolean;
  prefix?: string;
  db: () => Db;
  schema: BackendSchemaInput;
}

export const jazzAdapter = (config: JazzAdapterConfig) => {
  const prefix = config.prefix ?? "better_auth_";

  return createAdapterFactory({
    config: {
      adapterId: "jazz",
      adapterName: "Jazz Adapter",
      debugLogs: config.debugLogs,
      usePlural: config.usePlural,
      supportsBooleans: true,
      supportsDates: true,
      supportsJSON: true,
      supportsArrays: true,
      supportsNumericIds: false,
      supportsUUIDs: true,
      disableIdGeneration: true,
      transaction: false,
    },
    adapter: ({ schema, getModelName, getFieldName, getDefaultModelName }) => {
      const getPrefixedModelName = (model: string) => `${prefix}${getModelName(model)}`;
      const wasmSchema = resolveSchemaSource(config.schema);

      const applySelect = (row: JazzRowRecord | null, select?: string[]): JazzRowRecord | null => {
        if (row === null || select === undefined) {
          return row;
        }

        const selectedEntries = select.flatMap((field) =>
          Object.prototype.hasOwnProperty.call(row, field) ? [[field, row[field]]] : [],
        );

        return Object.fromEntries(selectedEntries);
      };

      type UniqueConstraint = {
        storedFieldNames: readonly string[];
      };

      const getUniqueConstraints = (model: string): UniqueConstraint[] => {
        const defaultModelName = getDefaultModelName(model);
        const modelSchema = schema[defaultModelName];
        if (!modelSchema) return [];

        const result: UniqueConstraint[] = [];
        for (const [fieldName, field] of Object.entries(modelSchema.fields)) {
          if (field.unique) {
            result.push({
              storedFieldNames: [getFieldName({ model: defaultModelName, field: fieldName })],
            });
          }
        }

        for (const index of readTableIndexes(defaultModelName, modelSchema.indexes)) {
          if (!index.unique) continue;
          result.push({
            storedFieldNames: index.fields.map((field) =>
              getFieldName({ model: defaultModelName, field }),
            ),
          });
        }

        return result;
      };

      const toQueryCondition = (model: string, condition: CleanedWhere): JazzBuiltCondition => {
        const column = getFieldName({ model, field: condition.field });

        if (condition.operator === "eq" && condition.value === null) {
          return {
            column,
            op: "isNull",
          };
        }

        return {
          column,
          op: condition.operator === "not_in" ? "notIn" : condition.operator,
          value: condition.value,
        };
      };

      const findAllRows = async (
        model: string,
        options: {
          where?: CleanedWhere[];
          sortBy?: JazzSortBy;
          limit?: number;
          offset?: number;
          forceClientSide?: boolean;
        } = {},
        readAll: (
          query: QueryBuilder<Record<string, unknown>>,
        ) => Promise<Record<string, unknown>[]> = (query) =>
          config.db().all(query, { tier: "global" }),
      ): Promise<JazzRowRecord[]> => {
        const table = getPrefixedModelName(model);

        const querySupportedByJazz = isQuerySupported(wasmSchema[table]!, options.where);

        if (querySupportedByJazz) {
          // Preserve ordering semantics: until sorting is lowered, pagination
          // must remain after the client-side sort. Predicate-only bounded reads
          // (including mutation/uniqueness warmups) can be bounded in Groove.
          const lowerPagination = options.sortBy === undefined;
          const qb = createQueryBuilder(table, wasmSchema, {
            conditions: (options.where ?? []).map((condition) =>
              toQueryCondition(model, condition),
            ),
            limit: lowerPagination ? options.limit : undefined,
            offset: lowerPagination ? options.offset : undefined,
          });

          let rows = (await readAll(qb)) as JazzRowRecord[];
          rows = sortListByField(rows, options.sortBy);
          if (!lowerPagination) {
            rows = paginateList(rows, options.limit, options.offset);
          }
          return rows;
        } else {
          console.warn(
            `Query not supported yet by Jazz engine: ${JSON.stringify(options.where?.map((c) => ({ ...c, value: typeof c.value === "string" ? "..." : c.value })))}`,
          );
        }

        const qb = createQueryBuilder(table, wasmSchema);

        let rows = (await readAll(qb)) as JazzRowRecord[];

        rows = filterListByWhere(rows, options.where);
        rows = sortListByField(rows, options.sortBy);
        rows = paginateList(rows, options.limit, options.offset);

        return rows;
      };

      const findByJazzRowId = async (model: string, jazzRowId: string) => {
        const table = getPrefixedModelName(model);

        return config.db().one(
          createQueryBuilder(table, wasmSchema, {
            conditions: [{ column: "id", op: "eq", value: jazzRowId }],
            limit: 1,
          }),
          { tier: "global" },
        );
      };

      const uniqueTupleFor = (
        data: Record<string, unknown>,
        constraint: UniqueConstraint,
      ): unknown[] | null => {
        const values = constraint.storedFieldNames.map((field) => data[field]);
        // Match ordinary SQL unique-index semantics: tuples containing NULL are
        // not equal to one another. `undefined` means Better Auth did not supply
        // this field, so there is no complete tuple to validate either.
        return values.some((value) => value === undefined || value === null) ? null : values;
      };

      const constraintName = (constraint: UniqueConstraint) =>
        constraint.storedFieldNames.join(", ");

      const assertUniqueConstraints = async (
        model: string,
        candidates: readonly JazzRowRecord[],
        excludeRowIds?: ReadonlySet<string>,
        readAll: (
          query: QueryBuilder<Record<string, unknown>>,
        ) => Promise<Record<string, unknown>[]> = (query) =>
          config.db().all(query, { tier: "global" }),
      ): Promise<void> => {
        const table = getPrefixedModelName(model);
        const uniqueConstraints = getUniqueConstraints(model);
        const excluded = excludeRowIds?.size ?? 0;
        const candidateTuples = new Map<string, string>();

        for (const candidate of candidates) {
          for (const constraint of uniqueConstraints) {
            const tuple = uniqueTupleFor(candidate, constraint);
            if (!tuple) continue;

            const tupleKey = JSON.stringify(tuple);
            const priorCandidateId = candidateTuples.get(
              `${constraintName(constraint)}\u0000${tupleKey}`,
            );
            if (priorCandidateId && priorCandidateId !== candidate.id) {
              throw new Error(
                `Unique constraint violated: "${table}.${constraintName(constraint)}" would have duplicate value tuple`,
              );
            }
            candidateTuples.set(`${constraintName(constraint)}\u0000${tupleKey}`, candidate.id);

            const checkQb = createQueryBuilder(table, wasmSchema, {
              conditions: constraint.storedFieldNames.map((column, index) => ({
                column,
                op: "eq" as const,
                value: tuple[index],
              })),
              limit: excluded + 1,
            });

            const existing = (await readAll(checkQb)) as JazzRowRecord[];
            const conflict = existing.find((row) => !excludeRowIds?.has(row.id));
            if (conflict) {
              throw new Error(
                `Unique constraint violated: "${table}.${constraintName(constraint)}" already has a row with value tuple ${JSON.stringify(tuple)}`,
              );
            }
          }
        }
      };

      const assertRowIdAvailable = async (
        model: string,
        id: string,
        readAll: (
          query: QueryBuilder<Record<string, unknown>>,
        ) => Promise<Record<string, unknown>[]> = (query) =>
          config.db().all(query, { tier: "global" }),
      ): Promise<void> => {
        const table = getPrefixedModelName(model);
        const existing = await readAll(
          createQueryBuilder(table, wasmSchema, {
            conditions: [{ column: "id", op: "eq", value: id }],
            limit: 1,
          }),
        );
        if (existing.length > 0) {
          throw new Error(`Unique constraint violated: row "${table}.${id}" already exists`);
        }
      };

      const isRetryableExclusiveConflict = (error: unknown) =>
        error instanceof PersistedWriteRejectedError &&
        (error.code === "cascade_rejected" ||
          error.code === "exclusive_conflict" ||
          error.code === "transaction_conflict");

      const runExclusiveMutation = async (
        model: string,
        where: CleanedWhere[],
        preflight: (match: JazzRowRecord) => Promise<void>,
        mutate: (
          tx: TransactionScope<"exclusive">,
          match: JazzRowRecord,
        ) => JazzRowRecord | Promise<JazzRowRecord>,
      ): Promise<JazzRowRecord | null> => {
        const db = config.db();

        while (true) {
          // Synchronize the relevant query before anchoring the exclusive snapshot. This also
          // initializes the Jazz client, which is required before starting an exclusive
          // transaction.
          const [globalMatch] = await findAllRows(model, { where, limit: 1 }, (query) =>
            db.all(query, { tier: "global" }),
          );
          if (!globalMatch) return null;
          await preflight(globalMatch);

          try {
            const result = await db.exclusiveTransaction(async (tx) => {
              const [match] = await findAllRows(model, { where, limit: 1 }, (query) =>
                tx.all(query, { tier: "local" }),
              );
              if (!match) {
                return null;
              }

              return mutate(tx, match);
            });

            return await result.wait();
          } catch (error) {
            if (isRetryableExclusiveConflict(error)) {
              continue;
            }

            throw error;
          }
        }
      };

      const db = config.db() as any;
      let exclusiveMutationTail = Promise.resolve();

      const serializeExclusiveMutation = async <T>(operation: () => Promise<T>): Promise<T> => {
        const previous = exclusiveMutationTail;
        let release = () => {};
        exclusiveMutationTail = new Promise<void>((resolve) => {
          release = resolve;
        });
        await previous;

        try {
          return await operation();
        } finally {
          release();
        }
      };

      return {
        async create({ model, data }): Promise<any> {
          const table = getPrefixedModelName(model);
          const { id, ...fields } = data as Record<string, unknown> & { id?: string };
          const qb = createQueryBuilder(table, wasmSchema);

          while (true) {
            // Do the potentially remote reads before opening the exclusive batch.
            // The same predicates are repeated locally below, where the authority
            // serializes the actual admission decision.
            await findAllRows(model, { limit: 1 });
            await assertUniqueConstraints(model, [{ id: "<new>", ...fields }]);
            if (id) await assertRowIdAvailable(model, id);

            try {
              const result = await db.exclusiveTransaction(
                async (tx: TransactionScope<"exclusive">) => {
                  await assertUniqueConstraints(
                    model,
                    [{ id: "<new>", ...fields }],
                    undefined,
                    (query) => tx.all(query, { tier: "local" }),
                  );
                  if (id) {
                    await assertRowIdAvailable(model, id, (query) =>
                      tx.all(query, { tier: "local" }),
                    );
                  }
                  return tx.insert(qb, fields, id ? { id } : undefined);
                },
              );
              return await result.wait();
            } catch (error) {
              if (isRetryableExclusiveConflict(error)) continue;
              throw error;
            }
          }
        },

        async findOne({ model, where, select, join }): Promise<any> {
          assertNativeJoinsDisabled(join);

          const [first] = await findAllRows(model, {
            where,
            limit: 1,
          });

          return applySelect(first ?? null, select);
        },

        async findMany({
          model,
          where,
          limit,
          select: _select,
          sortBy,
          offset,
          join,
        }): Promise<any[]> {
          assertNativeJoinsDisabled(join);

          const rows = await findAllRows(model, {
            where,
            sortBy,
            limit,
            offset,
          });

          return rows.map((row) => applySelect(row, _select));
        },

        async count({ model, where }) {
          return (await findAllRows(model, { where })).length;
        },

        async update({ model, where, update }): Promise<any> {
          const { id: _id, ...fields } = update as Record<string, unknown>;
          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);

          while (true) {
            const [globalMatch] = await findAllRows(model, { where, limit: 1 });
            if (!globalMatch) return null;
            await assertUniqueConstraints(
              model,
              [{ ...globalMatch, ...fields }],
              new Set([globalMatch.id]),
            );

            try {
              const result = await db.exclusiveTransaction(
                async (tx: TransactionScope<"exclusive">) => {
                  const [match] = await findAllRows(model, { where, limit: 1 }, (query) =>
                    tx.all(query, { tier: "local" }),
                  );
                  if (!match) return null;

                  await assertUniqueConstraints(
                    model,
                    [{ ...match, ...fields }],
                    new Set([match.id]),
                    (query) => tx.all(query, { tier: "local" }),
                  );
                  tx.update(qb, match.id, fields);
                  return match.id;
                },
              );
              const id = await result.wait();
              return id ? findByJazzRowId(model, id) : null;
            } catch (error) {
              if (isRetryableExclusiveConflict(error)) continue;
              throw error;
            }
          }
        },

        async updateMany({ model, where, update }) {
          const { id: _id, ...fields } = update as Record<string, unknown>;
          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);

          while (true) {
            const globalMatches = await findAllRows(model, { where });
            if (globalMatches.length === 0) return 0;
            await assertUniqueConstraints(
              model,
              globalMatches.map((match) => ({ ...match, ...fields })),
              new Set(globalMatches.map((match) => match.id)),
            );

            try {
              const result = await db.exclusiveTransaction(
                async (tx: TransactionScope<"exclusive">) => {
                  const matches = await findAllRows(model, { where }, (query) =>
                    tx.all(query, { tier: "local" }),
                  );
                  if (matches.length === 0) return 0;

                  await assertUniqueConstraints(
                    model,
                    matches.map((match) => ({ ...match, ...fields })),
                    new Set(matches.map((match) => match.id)),
                    (query) => tx.all(query, { tier: "local" }),
                  );
                  for (const match of matches) {
                    tx.update(qb, match.id, fields);
                  }
                  return matches.length;
                },
              );
              return await result.wait();
            } catch (error) {
              if (isRetryableExclusiveConflict(error)) continue;
              throw error;
            }
          }
        },

        async delete({ model, where }) {
          const [match] = await findAllRows(model, { where, limit: 1 });
          if (!match) {
            return;
          }

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);
          await db.delete(qb, match.id).wait({ tier: "global" });
        },

        async deleteMany({ model, where }) {
          const matches = await findAllRows(model, { where });
          if (matches.length === 0) {
            return 0;
          }

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);
          for (const match of matches) {
            await db.delete(qb, match.id).wait({ tier: "global" });
          }

          return matches.length;
        },

        async consumeOne<T>({
          model,
          where,
        }: {
          model: string;
          where: CleanedWhere[];
        }): Promise<T | null> {
          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);
          const consumed = await serializeExclusiveMutation(() =>
            runExclusiveMutation(
              model,
              where,
              async () => {},
              (tx, match) => {
                tx.delete(qb, match.id);
                return match;
              },
            ),
          );

          return consumed as T | null;
        },

        async incrementOne<T>({
          model,
          where,
          increment,
          set,
        }: {
          model: string;
          where: CleanedWhere[];
          increment: Record<string, number>;
          set?: Record<string, unknown>;
        }): Promise<T | null> {
          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);
          const updated = await serializeExclusiveMutation(() =>
            runExclusiveMutation(
              model,
              where,
              async (match) => {
                const fields: Record<string, unknown> = {};
                for (const [field, delta] of Object.entries(increment)) {
                  const current = match[field];
                  if (typeof current !== "number") {
                    throw new TypeError(
                      `Cannot increment non-numeric field "${table}.${field}" with value "${String(current)}"`,
                    );
                  }
                  fields[field] = current + delta;
                }
                Object.assign(fields, set);
                delete fields.id;
                await assertUniqueConstraints(
                  model,
                  [{ ...match, ...fields }],
                  new Set([match.id]),
                );
              },
              async (tx, match) => {
                const fields: Record<string, unknown> = {};
                for (const [field, delta] of Object.entries(increment)) {
                  const current = match[field];
                  if (typeof current !== "number") {
                    throw new TypeError(
                      `Cannot increment non-numeric field "${table}.${field}" with value "${String(current)}"`,
                    );
                  }
                  fields[field] = current + delta;
                }

                Object.assign(fields, set);
                delete fields.id;

                await assertUniqueConstraints(
                  model,
                  [{ ...match, ...fields }],
                  new Set([match.id]),
                  (query) => tx.all(query, { tier: "local" }),
                );
                tx.update(qb, match.id, fields);

                const persisted = await tx.one(
                  createQueryBuilder(table, wasmSchema, {
                    conditions: [{ column: "id", op: "eq", value: match.id }],
                    limit: 1,
                  }),
                  { tier: "local" },
                );
                if (!persisted) {
                  throw new Error(
                    `Updated row "${table}.${match.id}" disappeared inside transaction`,
                  );
                }

                return persisted as JazzRowRecord;
              },
            ),
          );

          return updated as T | null;
        },

        async createSchema({ file, tables }) {
          return createJazzSchemaSourceFile({
            file,
            tables,
            getModelName: getPrefixedModelName,
            getFieldName,
          });
        },
      };
    },
  });
};
