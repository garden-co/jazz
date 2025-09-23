import {
  CoList,
  pick,
  OrderByOptions,
  RefsToResolve,
  WhereFieldCondition,
  WhereOptions,
} from "../internal.js";

export const OrderByDirection = { ASC: "asc", DESC: "desc" } as const;
export type OrderByDirection =
  (typeof OrderByDirection)[keyof typeof OrderByDirection];

export const WhereComparisonOperators = {
  $eq: "$eq",
  $ne: "$ne",
  $gt: "$gt",
  $gte: "$gte",
  $lt: "$lt",
  $lte: "$lte",
} as const;
export type WhereComparisonOperator = keyof typeof WhereComparisonOperators;

export const WhereLogicalOperators = {
  $and: "$and",
  $or: "$or",
  $not: "$not",
} as const;
export type WhereLogicalOperator = keyof typeof WhereLogicalOperators;

export type WhereClause =
  | {
      field: string;
      operator: WhereComparisonOperator;
      value: unknown;
    }
  | {
      combinator: WhereLogicalOperator;
      conditions: WhereClause[];
    };

export type OrderByClause = {
  field: string;
  orderDirection: OrderByDirection;
};

/**
 * Query modifiers are operations that transform the result of loading a collection of CoValues.
 * The allow filtering, ordering and paginating the results.
 */
export type QueryModifiers = {
  where?: WhereClause;
  orderBy?: OrderByClause[];
  limit?: number;
  offset?: number;
};

export function parseQueryModifiers(
  resolveQuery: RefsToResolve<CoList>,
): QueryModifiers {
  const queryModifiers =
    typeof resolveQuery === "object" && resolveQuery !== null
      ? pick(resolveQuery, ["$where", "$orderBy", "$limit", "$offset"] as const)
      : {};
  if (Object.keys(queryModifiers).length > 0) {
    return {
      limit: queryModifiers?.$limit,
      offset: queryModifiers?.$offset,
      where: parseWhere(queryModifiers?.$where),
      orderBy: parseOrderBy(queryModifiers?.$orderBy),
    };
  } else {
    return {};
  }
}

function parseWhere(
  where: WhereOptions<any> | undefined,
): QueryModifiers["where"] {
  if (!where) return undefined;

  const topLevelKeys = Object.keys(where);

  const logicalOperatorKeys = topLevelKeys.filter(
    (key) => key in WhereLogicalOperators,
  );
  const logicalConditions = logicalOperatorKeys.map((key) => {
    const combinator = key as WhereLogicalOperator;
    const conditions = where[combinator];

    if (combinator === WhereLogicalOperators.$not) {
      const negatedCondition = parseWhere(conditions as WhereOptions<any>);
      return {
        combinator,
        conditions: negatedCondition ? [negatedCondition] : [],
      };
    } else {
      return {
        combinator,
        conditions: (conditions as WhereOptions<any>[])
          .map((cond) => parseWhere(cond))
          .filter((cond) => cond !== undefined),
      };
    }
  });

  const fieldConditionsKeys = topLevelKeys.filter(
    (key): key is keyof typeof where => !(key in WhereLogicalOperators),
  );
  const fieldConditions = Object.entries(
    pick(where, fieldConditionsKeys),
  ).flatMap(([field, filter]) => parseFieldConditions(field, filter));

  const allConditions = [...logicalConditions, ...fieldConditions];

  if (allConditions.length === 1) {
    return allConditions[0];
  }
  return {
    combinator: WhereLogicalOperators.$and,
    conditions: allConditions,
  };
}

function parseFieldConditions(
  field: string,
  filter: WhereFieldCondition<any>,
): WhereClause {
  if (typeof filter !== "object") {
    return {
      field,
      operator: WhereComparisonOperators.$eq,
      value: filter,
    };
  }

  const filterKeys = Object.keys(filter);

  const logicalFilters = filterKeys
    .filter((key): key is WhereLogicalOperator => key in WhereLogicalOperators)
    .map((logicalOperator) => {
      const conditions = filter[logicalOperator];

      if (logicalOperator === WhereLogicalOperators.$not) {
        return {
          combinator: logicalOperator,
          conditions: [parseFieldConditions(field, conditions)],
        };
      } else {
        const fieldConditions = conditions.map(
          (cond: WhereFieldCondition<any>) => parseFieldConditions(field, cond),
        );
        return {
          combinator: logicalOperator,
          conditions: fieldConditions,
        };
      }
    });

  const comparisonFilters = filterKeys
    .filter(
      (key): key is WhereComparisonOperator => key in WhereComparisonOperators,
    )
    .map((comparisonOperator) => ({
      field,
      operator: comparisonOperator,
      value: filter[comparisonOperator],
    }));

  const allFilters = [...logicalFilters, ...comparisonFilters];
  if (allFilters.length === 1) {
    return allFilters[0]!;
  }
  return {
    combinator: WhereLogicalOperators.$and,
    conditions: allFilters,
  };
}

function parseOrderBy(
  orderBy: OrderByOptions<any> | undefined,
): QueryModifiers["orderBy"] {
  const orderClauses = Object.entries(orderBy ?? {});
  return orderClauses.map(([field, orderDirection]) => ({
    field,
    orderDirection: orderDirection as OrderByDirection,
  }));
}

/**
 * Returns the fields that are used in `$where` and `$orderBy` query modifiers
 */
export function queryModifierFields(queryModifiers: QueryModifiers): string[] {
  return [
    ...(queryModifiers?.where ? whereClauseFields(queryModifiers?.where) : []),
    ...orderByClauseFields(queryModifiers.orderBy ?? []),
  ];
}

function whereClauseFields(where: WhereClause): string[] {
  if ("field" in where) {
    return [where.field];
  } else {
    return where.conditions.flatMap((condition) =>
      whereClauseFields(condition),
    );
  }
}

function orderByClauseFields(orderBy: OrderByClause[]): string[] {
  return orderBy.map((orderBy) => orderBy.field);
}

/**
 * Computes the query view for a CoList based on the query modifiers.
 * The query view is a mapping from the raw indexes to the queried indexes.
 */
export function computeQueryView(
  coList: CoList,
  queryModifiers: QueryModifiers,
): Record<number, number> {
  const { limit = Infinity, offset = 0, orderBy, where } = queryModifiers;
  const queriedFields = queryModifierFields(queryModifiers);
  const fieldAccessors = Object.fromEntries(
    queriedFields.map((field) => [field, coList.$jazz.fieldAccessor(field)]),
  );
  const rawIndexes =
    queriedFields.length > 0
      ? Object.keys(fieldAccessors[queriedFields[0]!]!).map(Number)
      : coList.$jazz.raw.entries().map((entry, i) => i);
  let filteredArrayIndexes = rawIndexes;
  if (where) {
    filteredArrayIndexes = filteredArrayIndexes.filter((rawIndex) =>
      evaluateWhereClause(where, rawIndex, fieldAccessors),
    );
  }
  let sortedArrayIndexes = filteredArrayIndexes;
  if (orderBy?.length) {
    sortedArrayIndexes = sortedArrayIndexes.toSorted((rawIdxA, rawIdxB) => {
      for (const { field, orderDirection } of orderBy) {
        const dir = orderDirection === "desc" ? -1 : 1;
        const aValue = fieldAccessors[field]?.[rawIdxA];
        const bValue = fieldAccessors[field]?.[rawIdxB];

        // Undefined values go last, regardless of order direction
        if (aValue === undefined && bValue === undefined) continue;
        if (aValue === undefined) return 1;
        if (bValue === undefined) return -1;

        if (aValue < bValue) return -1 * dir;
        if (aValue > bValue) return 1 * dir;
      }
      return 0;
    });
  }
  const paginatedArrayIndexes = sortedArrayIndexes
    .slice(offset, offset + limit)
    .map((rawIndex, idx) => [idx, rawIndex]);
  return Object.fromEntries(paginatedArrayIndexes);
}

function evaluateWhereClause(
  whereClause: WhereClause,
  coListRawIndex: number,
  fieldAccessors: Record<string, Record<number, any>>,
): boolean {
  if ("field" in whereClause) {
    const valueToFilter = fieldAccessors[whereClause.field]?.[coListRawIndex];
    return evaluateWhereFieldCondition(
      whereClause.operator,
      valueToFilter,
      whereClause.value,
    );
  } else {
    switch (whereClause.combinator) {
      case WhereLogicalOperators.$and:
        return whereClause.conditions.every((condition) =>
          evaluateWhereClause(condition, coListRawIndex, fieldAccessors),
        );
      case WhereLogicalOperators.$or:
        return whereClause.conditions.some((condition) =>
          evaluateWhereClause(condition, coListRawIndex, fieldAccessors),
        );
      case WhereLogicalOperators.$not:
        return !evaluateWhereClause(
          whereClause.conditions[0]!,
          coListRawIndex,
          fieldAccessors,
        );
    }
  }
}

function evaluateWhereFieldCondition(
  operator: WhereComparisonOperator,
  valueToFilter: any,
  value: any,
): boolean {
  switch (operator) {
    case WhereComparisonOperators.$eq:
      return valueToFilter === value;
    case WhereComparisonOperators.$ne:
      return valueToFilter !== value;
  }
  if (valueToFilter === undefined) return false;
  switch (operator) {
    case WhereComparisonOperators.$gt:
      return valueToFilter > value;
    case WhereComparisonOperators.$gte:
      return valueToFilter >= value;
    case WhereComparisonOperators.$lt:
      return valueToFilter < value;
    case WhereComparisonOperators.$lte:
      return valueToFilter <= value;
  }
}
