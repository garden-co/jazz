export interface ColumnTransform {
  /** Internal marker: a virtual field has no stored input or writable output. */
  readonly computed?: true;
  readonly rowColumns?: readonly string[];
  readonly dependsOn?: readonly string[];
  from(value: unknown, dependencies: Readonly<Record<string, unknown>>): unknown;
  to(value: unknown, dependencies: Readonly<Record<string, unknown>>): unknown;
}
export type ColumnTransformMap = Record<string, ColumnTransform>;
export type ColumnTransformRegistry = Record<string, ColumnTransformMap | undefined>;

type DependencyTransforms = Readonly<
  Record<string, Pick<ColumnTransform, "dependsOn" | "computed">>
>;
type DependencyPlan = {
  columns: readonly string[];
  byColumn: ReadonlyMap<string, readonly string[]>;
  hasWriteDependencies: boolean;
  computedColumns: readonly string[];
};
const dependencyPlans = new WeakMap<DependencyTransforms, DependencyPlan>();
const emptyContext: Readonly<Record<string, unknown>> = Object.freeze({});
const emptyDependencies: readonly string[] = Object.freeze([]);

/** The declarations are validated and copied when the table is constructed. */
export function columnTransformDependencies(transforms: DependencyTransforms): DependencyPlan {
  const cached = dependencyPlans.get(transforms);
  if (cached) return cached;
  const columns = new Set<string>();
  const byColumn = new Map<string, readonly string[]>();
  const visiting = new Set<string>();
  const visit = (column: string): readonly string[] => {
    const previous = byColumn.get(column);
    if (previous) return previous;
    if (visiting.has(column)) throw new Error(`Cyclic transform dependency at "${column}".`);
    const dependencies = transforms[column]?.dependsOn;
    if (!dependencies?.length) {
      byColumn.set(column, emptyDependencies);
      return emptyDependencies;
    }
    visiting.add(column);
    const closure = new Set<string>();
    for (const dependency of dependencies) {
      columns.add(dependency);
      for (const transitive of visit(dependency)) closure.add(transitive);
      closure.add(dependency);
    }
    visiting.delete(column);
    const result = [...closure];
    byColumn.set(column, result);
    return result;
  };
  for (const column of Object.keys(transforms)) visit(column);
  const computedColumns = Object.keys(transforms).filter((column) => transforms[column]!.computed);
  const plan = {
    columns: [...columns],
    byColumn,
    hasWriteDependencies: Object.values(transforms).some(
      (transform) => !transform.computed && !!transform.dependsOn?.length,
    ),
    computedColumns,
  };
  dependencyPlans.set(transforms, plan);
  return plan;
}

export function storedDependencyValues(
  columns: readonly string[],
  values: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> {
  if (!columns.length) return emptyContext;
  const snapshot: Record<string, unknown> = {};
  for (const column of columns)
    if (Object.hasOwn(values, column))
      Object.defineProperty(snapshot, column, { value: values[column], enumerable: true });
  return snapshot;
}

export function dependencyContext(
  transform: Pick<ColumnTransform, "dependsOn">,
  values: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> {
  if (!transform.dependsOn?.length) return emptyContext;
  const context: Record<string, unknown> = {};
  for (const column of transform.dependsOn) {
    if (!Object.hasOwn(values, column))
      throw new Error(`Transform dependency "${column}" is unavailable.`);
    Object.defineProperty(context, column, { value: values[column], enumerable: true });
  }
  return Object.freeze(context);
}

export function validateTransformDependencies(
  columns: Record<
    string,
    {
      _transform?: Pick<ColumnTransform, "dependsOn">;
      _computed?: unknown;
    }
  >,
): void {
  const complete = new Set<string>();
  const visiting = new Set<string>();
  const visit = (column: string) => {
    if (complete.has(column) || column === "id") return;
    if (visiting.has(column)) throw new Error(`Cyclic transform dependency at "${column}".`);
    visiting.add(column);
    const definition = columns[column];
    const dependencies = definition?._transform?.dependsOn ?? [];
    if (
      !Array.isArray(dependencies) ||
      dependencies.some((dependency) => typeof dependency !== "string")
    )
      throw new Error(`Transform dependencies for "${column}" must be column names.`);
    for (const dependency of dependencies) {
      if (dependency === column) throw new Error(`Transform "${column}" cannot depend on itself.`);
      if (dependency !== "id" && !Object.hasOwn(columns, dependency))
        throw new Error(`Transform "${column}" references unknown dependency "${dependency}".`);
      if (!definition?._computed && columns[dependency]?._computed)
        throw new Error(
          `Stored transform "${column}" cannot depend on computed field "${dependency}".`,
        );
      visit(dependency);
    }
    visiting.delete(column);
    complete.add(column);
  };
  for (const column of Object.keys(columns)) visit(column);
}

/** Private retry signal: a computation needs a complete stored column. */
export class MissingComputedDependency extends Error {
  constructor(column: string) {
    super(`Computed dependency "${column}" is unavailable.`);
  }
}

/** Observations belong to one read/subscription, never another session or branch. */
// Only column names are shared; definitions can be collected along with their hints.
const computedDependencyHints = new WeakMap<ColumnTransform, Set<string>>();

export class ComputedReadState {
  private readonly dependencies = new Map<ColumnTransform, Set<string>>();
  version = 0;

  private observedColumns(transform: ColumnTransform): Set<string> {
    let columns = this.dependencies.get(transform);
    if (!columns) {
      // Snapshot hints before planning: another read must not change this read's
      // dependencies without advancing its own discovery version.
      columns = new Set(computedDependencyHints.get(transform));
      this.dependencies.set(transform, columns);
    }
    return columns;
  }

  observe(transform: ColumnTransform, column: string): void {
    const columns = this.observedColumns(transform);
    if (!columns.has(column)) {
      columns.add(column);
      this.version++;
      let hints = computedDependencyHints.get(transform);
      if (!hints) computedDependencyHints.set(transform, (hints = new Set()));
      hints.add(column);
    }
  }

  storedColumns(transforms: ColumnTransformMap, requested: string): ReadonlySet<string> {
    const columns = new Set<string>();
    const visited = new Set<string>();
    const visit = (column: string) => {
      if (visited.has(column)) return;
      visited.add(column);
      const transform = Object.hasOwn(transforms, column) ? transforms[column] : undefined;
      if (transform?.computed) {
        for (const dependency of this.observedColumns(transform)) visit(dependency);
      } else {
        columns.add(column);
        // Stored transforms consume decoded stored context, not sibling app values.
        for (const dependency of transform?.dependsOn ?? []) columns.add(dependency);
      }
    };
    visit(requested);
    return columns;
  }
}

function containsComputedRow(value: unknown, row: object, seen?: Set<object>): boolean {
  if (value === row) return true;
  if (typeof value !== "object" || value === null || ArrayBuffer.isView(value) || seen?.has(value))
    return false;
  (seen ??= new Set()).add(value);
  if (value instanceof Map) {
    for (const [key, entry] of value)
      if (containsComputedRow(key, row, seen) || containsComputedRow(entry, row, seen)) return true;
  } else if (value instanceof Set) {
    for (const entry of value) if (containsComputedRow(entry, row, seen)) return true;
  }
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor && "value" in descriptor && containsComputedRow(descriptor.value, row, seen))
      return true;
  }
  return false;
}

/** Resolve app-facing values once, without evaluating hidden stored dependencies as outputs. */
export function computedColumnValues(
  transforms: ColumnTransformMap,
  requested: readonly string[],
  stored: Readonly<Record<string, unknown>>,
  state?: ComputedReadState,
  partial?: Readonly<Record<string, unknown>>,
): ReadonlyMap<string, unknown> {
  const values = new Map<string, unknown>();
  const visiting = new Set<string>();
  let missing: MissingComputedDependency | undefined;
  const requireStored = (column: string): unknown => {
    if (!Object.hasOwn(stored, column) || (partial && Object.hasOwn(partial, column))) {
      missing = new MissingComputedDependency(column);
      throw missing;
    }
    return stored[column];
  };
  const resolve = (column: string): unknown => {
    // Callbacks may catch a dependency miss; subsequent reads must preserve the retry signal.
    if (missing) throw missing;
    if (values.has(column)) return values.get(column);
    if (visiting.has(column)) throw new Error(`Cyclic computed dependency at "${column}".`);
    visiting.add(column);
    try {
      const transform = Object.hasOwn(transforms, column) ? transforms[column] : undefined;
      let value: unknown;
      if (transform?.computed) {
        const keys = transform.rowColumns ?? [];
        let snapshot: Record<string, unknown> | undefined;
        const readonly = () => {
          throw new TypeError("Computed callback rows are read-only.");
        };
        const read = (key: string) => {
          if (snapshot) return Reflect.get(snapshot, key);
          if (!keys.includes(key)) {
            // Honour ordinary object operations, while stored prototype-named
            // columns still take precedence over Object.prototype.
            if (Object.hasOwn(Object.prototype, key)) return Reflect.get(Object.prototype, key);
            if (key === "toJSON") return undefined;
            if (key === column) return resolve(key);
            throw new Error(`Unknown computed dependency "${key}".`);
          }
          state?.observe(transform, key);
          return resolve(key);
        };
        const row = new Proxy(Object.create(null) as Record<string, unknown>, {
          get: (_target, key) => (typeof key === "string" ? read(key) : undefined),
          has: (_target, key) => typeof key === "string" && keys.includes(key),
          ownKeys: () => [...keys],
          getOwnPropertyDescriptor: (_target, key) =>
            typeof key === "string" && keys.includes(key)
              ? { configurable: true, enumerable: true, get: () => read(key) }
              : undefined,
          set: readonly,
          deleteProperty: readonly,
          defineProperty: readonly,
          setPrototypeOf: readonly,
          preventExtensions: readonly,
        });
        try {
          value = transform.from(undefined, row);
        } catch (error) {
          // A callback may wrap the private signal while handling ordinary data errors.
          throw missing ?? error;
        }
        if (
          value &&
          (typeof value === "object" || typeof value === "function") &&
          "then" in value &&
          typeof value.then === "function"
        ) {
          // Async callbacks are unsupported, but their rejection must remain contained.
          Promise.resolve(value).catch(() => {});
          throw new TypeError(`Computed field "${column}" must return synchronously.`);
        }
        if (missing) throw missing;
        // Complete nested row references without cloning or mutating the callback's result.
        if (containsComputedRow(value, row)) {
          snapshot = { ...row };
          if (value === row) value = snapshot;
        }
      } else {
        const storedValue = requireStored(column);
        for (const dependency of transform?.dependsOn ?? []) requireStored(dependency);
        value = transform
          ? transform.from(storedValue, dependencyContext(transform, stored))
          : storedValue;
      }
      values.set(column, value);
      return value;
    } finally {
      visiting.delete(column);
    }
  };
  for (const column of requested) resolve(column);
  return values;
}
