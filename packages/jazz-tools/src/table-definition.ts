import { assertRelationshipDeclaration } from "./relationships.js";
import type { Relationships, ForwardRelationship } from "./relationships.js";
import type { AnyTypedColumnBuilder, ColumnBuilderSqlType } from "./dsl.js";
import { assertUserTableColumnNameAllowed, type NoExplicitIdColumn } from "./magic-columns.js";

export type TableDefinition = Record<string, AnyTypedColumnBuilder> & NoExplicitIdColumn;

// Wrap table columns so we can hang chained modifiers like .indexOnly(...) off tables
// without changing the column-level schema representation the runtime uses today.
export class DefinedTable<
  TColumns extends TableDefinition = TableDefinition,
  TRelations extends Relationships = Relationships,
> {
  public readonly __jazzTableDefinition = true as const;

  constructor(
    public readonly columns: TColumns,
    public readonly relations: TRelations,
    public readonly indexedColumns?: readonly Extract<keyof TColumns, string>[],
    public readonly branchColumns?: readonly Extract<keyof TColumns, string>[],
  ) {
    for (const column of Object.keys(columns)) assertUserTableColumnNameAllowed(column);
  }

  indexOnly<
    const TColumnsForIndex extends readonly [
      Extract<keyof TColumns, string>,
      ...Extract<keyof TColumns, string>[],
    ],
  >(columns: TColumnsForIndex): DefinedTable<TColumns, TRelations> {
    const normalizedColumns = [...columns] as Extract<keyof TColumns, string>[];
    for (const column of normalizedColumns) {
      if (!(column in this.columns)) {
        throw new Error(`table.indexOnly(...) references unknown column "${column}".`);
      }
    }

    return new DefinedTable(this.columns, this.relations, normalizedColumns, this.branchColumns);
  }

  branchBy<const TBranchColumn extends Extract<keyof TColumns, string>>(
    column: TBranchColumn,
  ): DefinedTable<TColumns, TRelations>;
  branchBy<
    const TBranchColumns extends readonly [
      Extract<keyof TColumns, string>,
      ...Extract<keyof TColumns, string>[],
    ],
  >(columns: TBranchColumns): DefinedTable<TColumns, TRelations>;
  branchBy(
    columns:
      | Extract<keyof TColumns, string>
      | readonly [Extract<keyof TColumns, string>, ...Extract<keyof TColumns, string>[]],
  ): DefinedTable<TColumns, TRelations> {
    const normalizedColumns = (Array.isArray(columns) ? [...columns] : [columns]) as Extract<
      keyof TColumns,
      string
    >[];
    for (const column of normalizedColumns) {
      if (!(column in this.columns)) {
        throw new Error(`table.branchBy(...) references unknown column "${column}".`);
      }
    }

    return new DefinedTable(this.columns, this.relations, this.indexedColumns, normalizedColumns);
  }
}

/**
 * Define a table with the given columns.
 *
 * @example
 * ```typescript
 * const schema = {
 *   todos: s.table({
 *     title: s.string(),
 *     done: s.boolean(),
 *   }),
 * });
 * type AppSchema = s.Schema<typeof schema>;
 * export const app: s.App<AppSchema> = s.defineApp(schema);
 * ```
 */
type RelationColumns<T extends TableDefinition> = {
  [K in keyof T & string]: ColumnBuilderSqlType<T[K]> extends
    | "UUID"
    | { kind: "ARRAY"; element: "UUID" }
    ? K
    : never;
}[keyof T & string];
type ConflictingTargets<R extends Relationships, C extends string, T extends string> = {
  [N in keyof R]: R[N] extends ForwardRelationship<infer OtherTarget, C>
    ? OtherTarget extends T
      ? never
      : N
    : never;
}[keyof R];
type ValidateLocalRelations<C extends TableDefinition, R extends Relationships> = {
  [K in keyof R]: K extends
    | keyof C
    | "id"
    | "__proto__"
    | "constructor"
    | "prototype"
    | ""
    | `$${string}`
    ? never
    : R[K] extends ForwardRelationship<infer Target, infer Col>
      ? Col extends RelationColumns<C>
        ? [ConflictingTargets<R, Col, Target>] extends [never]
          ? R[K]
          : never
        : never
      : R[K];
};
export function defineTable<
  const TColumns extends TableDefinition,
  const TRelations extends Relationships,
>(
  columns: TColumns,
  relations: TRelations & ValidateLocalRelations<TColumns, TRelations>,
): DefinedTable<TColumns, TRelations> {
  if (!relations || typeof relations !== "object" || Array.isArray(relations))
    throw new Error(
      "s.table(columns, relations) requires a relationship map; use {} for no relationships.",
    );
  const targets = new Map<string, string>();
  for (const [name, relation] of Object.entries(relations)) {
    if (
      !name ||
      ["__proto__", "constructor", "prototype"].includes(name) ||
      name.startsWith("$") ||
      name === "id" ||
      Object.hasOwn(columns, name)
    )
      throw new Error(`Relationship "${name}" collides with a column.`);
    assertRelationshipDeclaration(relation, name);
    if (relation.kind === "forward") {
      const previous = targets.get(relation.column);
      if (previous && previous !== relation.table)
        throw new Error(`Conflicting relationship targets for column "${relation.column}".`);
      targets.set(relation.column, relation.table);
      const column = Object.hasOwn(columns, relation.column)
        ? columns[relation.column]?._build(relation.column)
        : undefined;
      if (!column)
        throw new Error(`Relationship "${name}" references unknown column "${relation.column}".`);
      if (
        column.sqlType !== "UUID" &&
        !(
          typeof column.sqlType === "object" &&
          column.sqlType.kind === "ARRAY" &&
          column.sqlType.element === "UUID"
        )
      )
        throw new Error(
          `Relationship "${name}" requires a UUID or UUID[] column; "${relation.column}" is not one.`,
        );
    }
  }
  return new DefinedTable(columns, relations);
}
