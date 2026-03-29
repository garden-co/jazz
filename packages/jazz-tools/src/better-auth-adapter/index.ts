import {
  createAdapterFactory,
  DBAdapterDebugLogOption,
  type CleanedWhere,
} from "better-auth/adapters";
import type { Db, DurabilityTier, WasmSchema } from "jazz-tools";
import { buildJazzCurrentSchemaText } from "./schema.js";
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
  durabilityTier?: DurabilityTier;
  prefix?: string;
  db: () => Db;
  schema: WasmSchema;
}

export const jazzAdapter = (config: JazzAdapterConfig) => {
  const prefix = config.prefix ?? "better_auth_";
  const durabilityTier = config.durabilityTier ?? "edge";

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
    adapter: ({ schema, getModelName, getFieldName }) => {
      const getPrefixedModelName = (model: string) => `${prefix}${getModelName(model)}`;
      const wasmSchema = config.schema;

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

          return config.db().all(qb, { tier: durabilityTier }) as Promise<JazzRowRecord[]>;
        }

        const qb = createQueryBuilder(table, wasmSchema);

        let rows = (await config.db().all(qb, { tier: durabilityTier })) as JazzRowRecord[];

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
          { tier: durabilityTier },
        );
      };

      return {
        async create({ model, data }): Promise<any> {
          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);

          return config.db().insertDurable(qb, data, { tier: durabilityTier });
        },

        async findOne({ model, where, select, join }): Promise<any> {
          assertNativeJoinsDisabled(join);

          const [first] = await findAllRows(model, {
            where,
            limit: 1,
          });

          return first ?? null;
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

          return rows;
        },

        async count({ model, where }) {
          return (await findAllRows(model, { where })).length;
        },

        async update({ model, where, update }): Promise<any> {
          const [match] = await findAllRows(model, { where, limit: 1 });
          if (!match) {
            return null;
          }

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);

          await config.db().updateDurable(qb, match.id, update as any, { tier: durabilityTier });

          return findByJazzRowId(model, match.id);
        },

        async updateMany({ model, where, update }) {
          const matches = await findAllRows(model, { where });
          if (matches.length === 0) {
            return 0;
          }

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);

          for (const match of matches) {
            await config.db().updateDurable(qb, match.id, update, { tier: durabilityTier });
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
          await config.db().deleteDurable(qb, match.id, { tier: durabilityTier });
        },

        async deleteMany({ model, where }) {
          const matches = await findAllRows(model, { where });
          if (matches.length === 0) {
            return 0;
          }

          const table = getPrefixedModelName(model);
          const qb = createQueryBuilder(table, wasmSchema);
          for (const match of matches) {
            await config.db().deleteDurable(qb, match.id, { tier: durabilityTier });
          }

          return matches.length;
        },

        async createSchema({ file, tables }) {
          return {
            path: file ?? "./schema-better-auth/current.ts",
            overwrite: true,
            code: buildJazzCurrentSchemaText({
              tables,
              getModelName: getPrefixedModelName,
              getFieldName,
            }),
          };
        },
      };
    },
  });
};
