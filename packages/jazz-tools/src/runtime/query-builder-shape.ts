export interface BuiltCondition {
  column: string;
  op: string;
  value: unknown;
}

export interface BuiltGather {
  max_depth: number;
  step_table: string;
  step_current_column: string;
  step_conditions: BuiltCondition[];
  step_hops: string[];
}

export interface NormalizedIncludeEntry {
  table?: string;
  conditions: BuiltCondition[];
  includes: NormalizedIncludeSpec;
  select: string[];
  orderBy: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
  hops: string[];
  gather?: BuiltGather;
}

export interface NormalizedIncludeSpec {
  [relationName: string]: NormalizedIncludeEntry;
}

export interface NormalizedBuiltQuery {
  table: string;
  conditions: BuiltCondition[];
  includes: NormalizedIncludeSpec;
  select: string[];
  orderBy: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
  hops: string[];
  gather?: BuiltGather;
}

type BuiltQueryShape = {
  table?: unknown;
  conditions?: unknown;
  includes?: unknown;
  select?: unknown;
  orderBy?: unknown;
  limit?: unknown;
  offset?: unknown;
  hops?: unknown;
  gather?: unknown;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeConditions(value: unknown): BuiltCondition[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter(
    (condition): condition is BuiltCondition =>
      isPlainObject(condition) &&
      typeof condition.column === "string" &&
      typeof condition.op === "string",
  );
}

function normalizeOrderBy(value: unknown): Array<[string, "asc" | "desc"]> {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter(
    (entry): entry is [string, "asc" | "desc"] =>
      Array.isArray(entry) &&
      entry.length === 2 &&
      typeof entry[0] === "string" &&
      (entry[1] === "asc" || entry[1] === "desc"),
  );
}

function normalizeSelect(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  const select = value.filter((column): column is string => typeof column === "string");
  return select.includes("*") ? [] : select;
}

function normalizeGather(value: unknown): BuiltGather | undefined {
  const maxDepth =
    isPlainObject(value) && typeof value.max_depth === "number" ? value.max_depth : NaN;
  if (
    !isPlainObject(value) ||
    !Number.isInteger(maxDepth) ||
    maxDepth <= 0 ||
    typeof value.step_table !== "string" ||
    typeof value.step_current_column !== "string"
  ) {
    return undefined;
  }

  return {
    max_depth: maxDepth,
    step_table: value.step_table,
    step_current_column: value.step_current_column,
    step_conditions: normalizeConditions(value.step_conditions),
    step_hops: Array.isArray(value.step_hops)
      ? value.step_hops.filter((hop): hop is string => typeof hop === "string")
      : [],
  };
}

function createEmptyIncludeEntry(): NormalizedIncludeEntry {
  return {
    conditions: [],
    includes: {},
    select: [],
    orderBy: [],
    hops: [],
  };
}

function isBuiltQueryShape(value: Record<string, unknown>): value is BuiltQueryShape {
  return "table" in value && "conditions" in value && "includes" in value && "orderBy" in value;
}

function isNormalizedIncludeEntryShape(value: Record<string, unknown>): boolean {
  return "conditions" in value && "includes" in value && "select" in value && "orderBy" in value;
}

function normalizeIncludeEntry(raw: unknown): NormalizedIncludeEntry | null {
  if (raw === true) {
    return createEmptyIncludeEntry();
  }

  if (!isPlainObject(raw)) {
    return null;
  }

  if (isBuiltQueryShape(raw)) {
    const normalized = normalizeBuiltQuery(raw, "");
    return {
      table: normalized.table || undefined,
      conditions: normalized.conditions,
      includes: normalized.includes,
      select: normalized.select,
      orderBy: normalized.orderBy,
      limit: normalized.limit,
      offset: normalized.offset,
      hops: normalized.hops,
      gather: normalized.gather,
    };
  }

  if (isNormalizedIncludeEntryShape(raw)) {
    return {
      table: typeof raw.table === "string" ? raw.table : undefined,
      conditions: normalizeConditions(raw.conditions),
      includes: normalizeIncludeEntries(raw.includes),
      select: normalizeSelect(raw.select),
      orderBy: normalizeOrderBy(raw.orderBy),
      limit: typeof raw.limit === "number" ? raw.limit : undefined,
      offset: typeof raw.offset === "number" ? raw.offset : undefined,
      hops: Array.isArray(raw.hops)
        ? raw.hops.filter((hop): hop is string => typeof hop === "string")
        : [],
      gather: normalizeGather(raw.gather),
    };
  }

  const entry = createEmptyIncludeEntry();
  entry.includes = normalizeIncludeEntries(raw);
  return entry;
}

export function normalizeIncludeEntries(raw: unknown): NormalizedIncludeSpec {
  if (!isPlainObject(raw)) {
    return {};
  }

  const includes: NormalizedIncludeSpec = {};
  for (const [relationName, spec] of Object.entries(raw)) {
    if (!spec) {
      continue;
    }
    const normalized = normalizeIncludeEntry(spec);
    if (normalized) {
      includes[relationName] = normalized;
    }
  }

  return includes;
}

export function normalizeBuiltQuery(raw: unknown, fallbackTable: string): NormalizedBuiltQuery {
  const value = isPlainObject(raw) ? raw : {};

  return {
    table: typeof value.table === "string" && value.table.length > 0 ? value.table : fallbackTable,
    conditions: normalizeConditions(value.conditions),
    includes: normalizeIncludeEntries(value.includes),
    select: normalizeSelect(value.select),
    orderBy: normalizeOrderBy(value.orderBy),
    limit: typeof value.limit === "number" ? value.limit : undefined,
    offset: typeof value.offset === "number" ? value.offset : undefined,
    hops: Array.isArray(value.hops)
      ? value.hops.filter((hop): hop is string => typeof hop === "string")
      : [],
    gather: normalizeGather(value.gather),
  };
}
