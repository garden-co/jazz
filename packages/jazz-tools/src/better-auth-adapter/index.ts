import {
  createAdapterFactory,
  DBAdapterDebugLogOption,
  type CleanedWhere,
} from "better-auth/adapters";
import type { Db } from "../runtime/db.js";
import type { BackendSchemaInput } from "../backend/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { createJazzSchemaSourceFile } from "./schema.js";
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

      const getUniqueFields = (model: string): Array<{ storedFieldName: string }> => {
        const defaultModelName = getDefaultModelName(model);
        const modelSchema = schema[defaultModelName];
        if (!modelSchema) return [];

        const result: Array<{ storedFieldName: string }> = [];
        for (const [fieldName, field] of Object.entries(modelSchema.fields)) {
          if (field.unique) {
            result.push({
              storedFieldName: getFieldName({ model: defaultModelName, field: fieldName }),
            });
          }
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
          op: condition.operator,
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
      ): Promise<JazzRowRecord[]> => {
        const table = getPrefixedModelName(model);

        const querySupportedByJazz = isQuerySupported(wasmSchema[table]!, options.where);

        if (querySupportedByJazz) {
          const qb = createQueryBuilder(table, wasmSchema, {
            conditions: (options.where ?? []).map((condition) =>
              toQueryCondition(model, condition),
            ),
            orderBy: options.sortBy
              ? {
                  field: getFieldName({ model, field: options.sortBy.field }),
                  direction: options.sortBy.direction,
                }
              : undefined,
            limit: options.limit,
            offset: options.offset,
          });

          return config.db().all(qb, { tier: "global" }) as Promise<JazzRowRecord[]>;
        } else {
          console.warn(
            `Query not supported yet by Jazz engine: ${JSON.stringify(options.where?.map((c) => ({ ...c, value: typeof c.value === "string" ? "..." : c.value })))}`,
          );
        }

        const qb = createQueryBuilder(table, wasmSchema);

        let rows = (await config.db().all(qb, { tier: "global" })) as JazzRowRecord[];

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

      const assertUniqueConstraints = async (
        model: string,
        data: Record<string, unknown>,
        excludeRowIds?: ReadonlySet<string>,
      ): Promise<void> => {
        const table = getPrefixedModelName(model);
        const uniqueFields = getUniqueFields(model);
        const excluded = excludeRowIds?.size ?? 0;
        for (const { storedFieldName } of uniqueFields) {
          if (!Object.prototype.hasOwnProperty.call(data, storedFieldName)) continue;
          const value = data[storedFieldName];
          if (value === undefined || value === null) continue;

          const checkQb = createQueryBuilder(table, wasmSchema, {
            conditions: [{ column: storedFieldName, op: "eq", value }],
            limit: excluded + 1,
          });

          const existing = (await config.db().all(checkQb, { tier: "global" })) as JazzRowRecord[];
          const conflict = existing.find((row) => !excludeRowIds?.has(row.id));
          if (conflict) {
            throw new Error(
              `Unique constraint violated: "${table}.${storedFieldName}" already has a row with value "${String(value)}"`,
            );
          }
        }
      };

      const db = config.db() as any;

      return {
        async create({ model, data }): Promise<any> {
          const table = getPrefixedModelName(model);

          await assertUniqueConstraints(model, data as Record<string, unknown>);

          const { id, ...fields } = data as Record<string, unknown> & { id?: string };
          const qb = createQueryBuilder(table, wasmSchema);
          return db.insertDurable(qb, fields, { tier: "global", ...(id ? { id } : {}) });
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
          const [match] = await findAllRows(model, { where, limit: 1 });
          if (!match) {
            return null;
          }

          const { id: _id, ...fields } = update as Record<string, unknown>;

          await assertUniqueConstraints(model, fields, new Set([match.id]));

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);

          await db.updateDurable(qb, match.id, fields, { tier: "global" });

          return findByJazzRowId(model, match.id);
        },

        async updateMany({ model, where, update }) {
          const matches = await findAllRows(model, { where });
          if (matches.length === 0) {
            return 0;
          }

          const { id: _id, ...fields } = update as Record<string, unknown>;

          await assertUniqueConstraints(model, fields, new Set(matches.map((match) => match.id)));

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);

          for (const match of matches) {
            await db.updateDurable(qb, match.id, fields, { tier: "global" });
          }

          return matches.length;
        },

        async delete({ model, where }) {
          const [match] = await findAllRows(model, { where, limit: 1 });
          if (!match) {
            return;
          }

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);
          await db.deleteDurable(qb, match.id, { tier: "global" });
        },

        async deleteMany({ model, where }) {
          const matches = await findAllRows(model, { where });
          if (matches.length === 0) {
            return 0;
          }

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);
          for (const match of matches) {
            await db.deleteDurable(qb, match.id, { tier: "global" });
          }

          return matches.length;
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
