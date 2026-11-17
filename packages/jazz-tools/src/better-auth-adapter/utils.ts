import type { CleanedWhere } from "better-auth/adapters";
import type { QueryBuilder, TableProxy } from "../runtime/db.js";
import type { WasmSchema } from "../drivers/types.js";
import {
  getSupportedWhereOperatorsForSchemaColumn,
  type WhereOperator,
} from "../where-operators.js";
import type { JazzBuiltCondition, JazzSortBy } from "./types.js";

export function assertNativeJoinsDisabled(join?: unknown): void {
  if (join && Object.keys(join).length > 0) {
    throw new Error(
      "Jazz adapter does not support native joins. Leave Better Auth experimental joins disabled.",
    );
  }
}

/**
 * Thrown when Better Auth asks for a `where` shape the Jazz engine cannot
 * answer at a cost proportional to the rows it touches. The adapter refuses
 * it rather than loading the whole table and filtering in JavaScript.
 *
 * Detect it by `code`, which is stable.
 */
export class UnsupportedBetterAuthQueryError extends Error {
  override readonly name = "UnsupportedBetterAuthQueryError";
  readonly code = "unsupported_better_auth_query";

  constructor(table: string, pattern: string) {
    super(
      `Reading "${table}" with ${pattern} is not supported by the Jazz Better Auth adapter yet`,
    );
  }
}

/**
 * A Better Auth `where`, split into what the engine evaluates and the exact
 * prefix/suffix checks the adapter applies to the engine's result.
 *
 * `starts_with`/`ends_with` are lowered to the engine's `contains` (a
 * superset), so the residual only ever sees rows that contain the value.
 */
export interface BetterAuthQueryPlan {
  conditions: CleanedWhere[];
  residual: CleanedWhere[];
}

function describeCondition(condition: CleanedWhere): string {
  return `"${condition.operator}" on "${condition.field}"`;
}

/**
 * Splits `where` into engine conditions and a prefix/suffix residual, or
 * throws {@link UnsupportedBetterAuthQueryError} for shapes the engine
 * does not support (OR connectors, operators a column does not support,
 * `null` comparisons the engine refuses, case-insensitive string matches).
 */
export function planBetterAuthQuery(
  table: string,
  tableSchema: WasmSchema[string],
  where: CleanedWhere[] = [],
): BetterAuthQueryPlan {
  const columnByName = new Map(tableSchema.columns.map((column) => [column.name, column] as const));
  const conditions: CleanedWhere[] = [];
  const residual: CleanedWhere[] = [];

  where.forEach((condition, index) => {
    // Better Auth folds conditions left to right; the first one's connector joins nothing.
    if (index > 0 && condition.connector === "OR") {
      throw new UnsupportedBetterAuthQueryError(table, "an OR connector");
    }

    const values = Array.isArray(condition.value) ? condition.value : [condition.value];
    if (condition.mode === "insensitive" && values.some((value) => typeof value === "string")) {
      throw new UnsupportedBetterAuthQueryError(
        table,
        `case-insensitive ${describeCondition(condition)}`,
      );
    }

    const column = columnByName.get(condition.field);
    const supportedOperators = getSupportedWhereOperatorsForSchemaColumn(condition.field, column);

    if (condition.operator === "starts_with" || condition.operator === "ends_with") {
      if (typeof condition.value !== "string" || !supportedOperators?.includes("contains")) {
        throw new UnsupportedBetterAuthQueryError(table, describeCondition(condition));
      }
      conditions.push({ ...condition, operator: "contains" });
      residual.push(condition);
      return;
    }

    const jazzOperator = condition.operator === "not_in" ? "notIn" : condition.operator;
    if (!supportedOperators?.includes(jazzOperator as WhereOperator)) {
      throw new UnsupportedBetterAuthQueryError(table, describeCondition(condition));
    }

    if (condition.value === null) {
      if (!column?.nullable) {
        throw new UnsupportedBetterAuthQueryError(
          table,
          `a null comparison on non-nullable "${condition.field}"`,
        );
      }
      if (condition.operator === "ne" && column.references) {
        throw new UnsupportedBetterAuthQueryError(
          table,
          `"ne" null on reference "${condition.field}"`,
        );
      }
    }

    conditions.push(condition);
  });

  return { conditions, residual };
}

/** Applies the exact `starts_with`/`ends_with` checks of a {@link BetterAuthQueryPlan}. */
export function matchesResidual(row: Record<string, unknown>, residual: CleanedWhere[]): boolean {
  return residual.every(({ field, operator, value }) => {
    const cell = row[field];
    if (typeof cell !== "string" || typeof value !== "string") return false;
    return operator === "starts_with" ? cell.startsWith(value) : cell.endsWith(value);
  });
}

export function createQueryBuilder(
  table: string,
  schema: WasmSchema,
  options: {
    conditions?: JazzBuiltCondition[];
    orderBy?: JazzSortBy;
    limit?: number;
    offset?: number;
  } = {},
): QueryBuilder<Record<string, unknown>> &
  TableProxy<Record<string, unknown>, Record<string, unknown>> {
  return {
    _table: table,
    _schema: schema,
    _rowType: undefined as never,
    _initType: undefined as never,
    _build() {
      return JSON.stringify({
        table,
        conditions: options.conditions ?? [],
        includes: {},
        orderBy: options.orderBy ? [[options.orderBy.field, options.orderBy.direction]] : [],
        limit: options.limit,
        offset: options.offset,
      });
    },
  };
}

export function paginateList<T>(
  data: T[],
  limit: number | undefined,
  offset: number | undefined,
): T[] {
  if (offset === undefined && limit === undefined) {
    return data;
  }

  if (limit === 0) {
    return [];
  }

  let start = offset ?? 0;
  if (start < 0) {
    start = 0;
  }

  const end = limit ? start + limit : undefined;
  return data.slice(start, end);
}

function isWhereByField(field: string, where: CleanedWhere): boolean {
  return where.field === field && where.operator === "eq" && where.connector === "AND";
}

type SingleStringEqWhere<T extends string> = CleanedWhere & {
  field: T;
  operator: "eq";
  value: string;
  connector: "AND";
};

function isStringEqWhereByField<T extends string>(
  field: T,
  where: CleanedWhere,
): where is SingleStringEqWhere<T> {
  return isWhereByField(field, where) && typeof where.value === "string";
}

export function isWhereBySingleField<T extends string>(
  field: T,
  where: CleanedWhere[] | undefined,
): where is [SingleStringEqWhere<T>] {
  if (where === undefined || where.length !== 1) {
    return false;
  }

  const [cond] = where;
  if (!cond) {
    return false;
  }

  return isStringEqWhereByField(field, cond);
}
