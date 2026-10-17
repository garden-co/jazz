export interface ColumnTransform {
  readonly dependsOn?: readonly string[];
  from(value: unknown, dependencies: Readonly<Record<string, unknown>>): unknown;
  to(value: unknown, dependencies: Readonly<Record<string, unknown>>): unknown;
}
export type ColumnTransformMap = Record<string, ColumnTransform>;
export type ColumnTransformRegistry = Record<string, ColumnTransformMap | undefined>;

type DependencyTransforms = Readonly<Record<string, Pick<ColumnTransform, "dependsOn">>>;
type DependencyPlan = {
  columns: readonly string[];
  byColumn: ReadonlyMap<string, readonly string[]>;
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
  const visit = (column: string): readonly string[] => {
    const previous = byColumn.get(column);
    if (previous) return previous;
    const dependencies = transforms[column]?.dependsOn;
    if (!dependencies?.length) {
      byColumn.set(column, emptyDependencies);
      return emptyDependencies;
    }
    const closure = new Set<string>();
    for (const dependency of dependencies) {
      columns.add(dependency);
      for (const transitive of visit(dependency)) closure.add(transitive);
      closure.add(dependency);
    }
    const result = [...closure];
    byColumn.set(column, result);
    return result;
  };
  for (const column of Object.keys(transforms)) visit(column);
  const plan = { columns: [...columns], byColumn };
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
  columns: Record<string, { _transform?: Pick<ColumnTransform, "dependsOn"> }>,
): void {
  const complete = new Set<string>();
  const visiting = new Set<string>();
  const visit = (column: string) => {
    if (complete.has(column) || column === "id") return;
    if (visiting.has(column)) throw new Error(`Cyclic transform dependency at "${column}".`);
    visiting.add(column);
    const dependencies = columns[column]?._transform?.dependsOn ?? [];
    if (
      !Array.isArray(dependencies) ||
      dependencies.some((dependency) => typeof dependency !== "string")
    )
      throw new Error(`Transform dependencies for "${column}" must be column names.`);
    for (const dependency of dependencies) {
      if (dependency === column) throw new Error(`Transform "${column}" cannot depend on itself.`);
      if (dependency !== "id" && !Object.hasOwn(columns, dependency))
        throw new Error(`Transform "${column}" references unknown dependency "${dependency}".`);
      visit(dependency);
    }
    visiting.delete(column);
    complete.add(column);
  };
  for (const column of Object.keys(columns)) visit(column);
}
