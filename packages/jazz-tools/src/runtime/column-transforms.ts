export interface ColumnTransform {
  readonly dependsOn?: readonly string[];
  from(value: unknown, dependencies: Readonly<Record<string, unknown>>): unknown;
  to(value: unknown, dependencies: Readonly<Record<string, unknown>>): unknown;
}
export type ColumnTransformMap = Record<string, ColumnTransform>;
export type ColumnTransformRegistry = Record<string, ColumnTransformMap | undefined>;

export function dependencyContext(
  transform: Pick<ColumnTransform, "dependsOn">,
  values: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> {
  const context: Record<string, unknown> = {};
  for (const column of transform.dependsOn ?? []) {
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
