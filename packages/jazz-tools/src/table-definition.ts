import { assertRelationshipDeclaration } from "./relationships.js";
import type { Relationships, ForwardRelationship } from "./relationships.js";
import type {
  AnyTypedColumnBuilder,
  ColumnBuilderInitValue,
  ColumnBuilderOptional,
  ColumnBuilderSqlType,
} from "./dsl.js";
import {
  assertUserTableColumnNameAllowed,
  isProvenanceAccountColumn,
  type NoExplicitIdColumn,
  type ProvenanceAccountColumn,
} from "./magic-columns.js";
import { validateTransformDependencies } from "./runtime/column-transforms.js";
import type { TSTypeFromSqlType } from "./schema.js";
import { equalityIndexColumn, type EncryptionDeclaration } from "./e2ee/encrypted-schema.js";
export type TableDefinition = Record<string, AnyTypedColumnBuilder> & NoExplicitIdColumn;

type StoredDependencyValue<C extends AnyTypedColumnBuilder> =
  ColumnBuilderOptional<C> extends true
    ? TSTypeFromSqlType<ColumnBuilderSqlType<C>> | null
    : TSTypeFromSqlType<ColumnBuilderSqlType<C>>;
type DependencyValues<C extends TableDefinition, D extends string, Stored extends boolean> = {
  readonly [K in D]: K extends "id"
    ? string
    : K extends keyof C
      ? Stored extends true
        ? StoredDependencyValue<C[K]>
        : ColumnBuilderOptional<C[K]> extends true
          ? ColumnBuilderInitValue<C[K]> | null
          : ColumnBuilderInitValue<C[K]>
      : never;
};
type ValidDependencyContext<
  C extends TableDefinition,
  D extends string,
  Context,
  Stored extends boolean,
> =
  Exclude<keyof Context, D> extends never
    ? DependencyValues<C, D, Stored> extends Context
      ? true
      : false
    : false;
export type ValidateTransformContexts<C extends TableDefinition> = {
  [K in keyof C]: C[K] extends {
    readonly __jazzDependencies: readonly (infer D extends string)[];
    readonly __jazzStoredDependencies: infer S;
    readonly __jazzInputDependencies: infer I;
  }
    ? string extends D
      ? C[K]
      : Exclude<D, keyof C | "id"> extends never
        ? K extends D
          ? never
          : ValidDependencyContext<C, D, S, true> extends true
            ? ValidDependencyContext<C, D, I, false> extends true
              ? C[K]
              : never
            : never
        : never
    : C[K];
};

// Wrap table columns so we can hang chained modifiers like .indexOnly(...) off tables
// without changing the column-level schema representation the runtime uses today.
export class DefinedTable<
  TColumns extends TableDefinition = TableDefinition,
  TRelations extends Relationships = Relationships,
  TEncryption extends EncryptionDeclaration | undefined = EncryptionDeclaration | undefined,
> {
  public readonly __jazzTableDefinition = true as const;

  encrypted(options: {
    space: Extract<keyof TColumns, string>;
    columns?: readonly Extract<keyof TColumns, string>[];
    indexes?: Partial<Record<Extract<keyof TColumns, string>, "equality">>;
  }): DefinedTable<TColumns, TRelations, EncryptionDeclaration> {
    const referenceColumns = new Set<string>();
    for (const relation of Object.values(this.relations)) {
      if (relation.kind === "forward") referenceColumns.add(relation.column);
    }
    const selected =
      options.columns ?? Object.keys(this.columns).filter((name) => !referenceColumns.has(name));
    for (const name of selected) {
      if (!(name in this.columns)) throw new Error(`Unknown encrypted column "${name}"`);
      if (referenceColumns.has(name)) {
        throw new Error(`Encrypted column "${name}" must not be a reference`);
      }
      const column = this.columns[name]!._build(name);
      if (column.default !== undefined || column.mergeStrategy !== undefined)
        throw new Error(
          `Encrypted column "${name}" cannot have a server default or merge strategy`,
        );
      if (
        this.indexedColumns?.some((key) => key === name) ||
        this.branchColumns?.some((key) => key === name) ||
        this.compositeIndexes?.some((index) => index.some((column) => column === name))
      )
        throw new Error(`Encrypted column "${name}" cannot be an ordinary index or branch key`);
    }
    const space = this.columns[options.space]?._build(options.space);
    if (
      options.indexes !== undefined &&
      (options.indexes === null ||
        typeof options.indexes !== "object" ||
        Array.isArray(options.indexes))
    )
      throw new Error("Encrypted indexes must map column names to equality");
    const indexes: Record<string, "equality"> = Object.create(null);
    for (const [name, mode] of Object.entries(options.indexes ?? {})) {
      if (!selected.some((column) => column === name))
        throw new Error(`Index column "${name}" must be encrypted`);
      if (mode !== "equality") throw new Error(`Encrypted index "${name}" only supports equality`);
      indexes[name] = mode;
      if (Object.hasOwn(this.columns, equalityIndexColumn(name)))
        throw new Error(`Column "${equalityIndexColumn(name)}" conflicts with an encrypted index`);
    }
    if (
      !referenceColumns.has(options.space) ||
      !space ||
      space.sqlType !== "UUID" ||
      space.nullable
    )
      throw new Error("An encryption space must be selected by a required UUID reference");
    return new DefinedTable(
      this.columns,
      this.relations,
      this.indexedColumns,
      this.branchColumns,
      this.compositeIndexes,
      {
        space: options.space,
        columns: [...new Set(selected)],
        indexes,
      },
    );
  }

  constructor(
    public readonly columns: TColumns,
    public readonly relations: TRelations,
    public readonly indexedColumns?: readonly Extract<keyof TColumns, string>[],
    public readonly branchColumns?: readonly Extract<keyof TColumns, string>[],
    public readonly compositeIndexes?: readonly (readonly Extract<keyof TColumns, string>[])[],
    public readonly encryption?: TEncryption,
  ) {
    validateTransformDependencies(columns);
    for (const column of Object.keys(columns)) assertUserTableColumnNameAllowed(column);
  }

  indexOnly<
    const TColumnsForIndex extends readonly [
      Extract<keyof TColumns, string>,
      ...Extract<keyof TColumns, string>[],
    ],
  >(columns: TColumnsForIndex): DefinedTable<TColumns, TRelations, TEncryption> {
    const normalizedColumns = [...columns] as Extract<keyof TColumns, string>[];
    for (const column of normalizedColumns) {
      if (!(column in this.columns)) {
        throw new Error(`table.indexOnly(...) references unknown column "${column}".`);
      }
    }

    if (normalizedColumns.some((name) => this.encryption?.columns.includes(name)))
      throw new Error("Encrypted columns cannot be ordinary index keys");
    return new DefinedTable(
      this.columns,
      this.relations,
      normalizedColumns,
      this.branchColumns,
      this.compositeIndexes,
      this.encryption,
    );
  }

  compositeIndex<
    const TColumnsForIndex extends readonly [
      Extract<keyof TColumns, string>,
      Extract<keyof TColumns, string>,
      ...Extract<keyof TColumns, string>[],
    ],
  >(columns: TColumnsForIndex): DefinedTable<TColumns, TRelations, TEncryption> {
    if (
      new Set(columns).size !== columns.length ||
      columns.some((column) => !(column in this.columns))
    ) {
      throw new Error("table.compositeIndex(...) requires distinct declared columns.");
    }
    if (columns.some((name) => this.encryption?.columns.includes(name)))
      throw new Error("Encrypted columns cannot be ordinary index keys");
    const key = JSON.stringify(columns);
    if ((this.compositeIndexes ?? []).some((existing) => JSON.stringify(existing) === key)) {
      throw new Error("table.compositeIndex(...) declares the same index twice.");
    }
    return new DefinedTable(
      this.columns,
      this.relations,
      this.indexedColumns,
      this.branchColumns,
      [...(this.compositeIndexes ?? []), [...columns]],
      this.encryption,
    );
  }

  branchBy<const TBranchColumn extends Extract<keyof TColumns, string>>(
    column: TBranchColumn,
  ): DefinedTable<TColumns, TRelations, TEncryption>;
  branchBy<
    const TBranchColumns extends readonly [
      Extract<keyof TColumns, string>,
      ...Extract<keyof TColumns, string>[],
    ],
  >(columns: TBranchColumns): DefinedTable<TColumns, TRelations, TEncryption>;
  branchBy(
    columns:
      | Extract<keyof TColumns, string>
      | readonly [Extract<keyof TColumns, string>, ...Extract<keyof TColumns, string>[]],
  ): DefinedTable<TColumns, TRelations, TEncryption> {
    const normalizedColumns = (Array.isArray(columns) ? [...columns] : [columns]) as Extract<
      keyof TColumns,
      string
    >[];
    for (const column of normalizedColumns) {
      if (!(column in this.columns)) {
        throw new Error(`table.branchBy(...) references unknown column "${column}".`);
      }
    }

    if (normalizedColumns.some((name) => this.encryption?.columns.includes(name)))
      throw new Error("Encrypted columns cannot be branch keys");
    return new DefinedTable(
      this.columns,
      this.relations,
      this.indexedColumns,
      normalizedColumns,
      this.compositeIndexes,
      this.encryption,
    );
  }
}

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
      ? Col extends RelationColumns<C> | ProvenanceAccountColumn
        ? [ConflictingTargets<R, Col, Target>] extends [never]
          ? R[K]
          : never
        : never
      : R[K];
};
/**
 * Define a table with explicit columns and named relationships.
 *
 * @example
 * ```typescript
 * const schema = {
 *   todos: s.table({
 *     title: s.string(),
 *     done: s.boolean(),
 *   }, {}),
 * };
 * type AppSchema = s.Schema<typeof schema>;
 * export const app: s.App<AppSchema> = s.defineApp(schema);
 * ```
 */
export function defineTable<
  const TColumns extends TableDefinition,
  const TRelations extends Relationships,
>(
  columns: TColumns & ValidateTransformContexts<TColumns>,
  relations: TRelations & ValidateLocalRelations<TColumns, TRelations>,
): DefinedTable<TColumns, TRelations, undefined> {
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
      if (isProvenanceAccountColumn(relation.column)) continue;
      const column = Object.hasOwn(columns, relation.column)
        ? columns[relation.column]?._build(relation.column)
        : undefined;
      if (!column)
        throw new Error(`Relationship "${name}" references unknown column "${relation.column}".`);
      if (
        typeof column.sqlType === "object" &&
        column.sqlType.kind === "ARRAY" &&
        typeof column.sqlType.element === "object" &&
        column.sqlType.element.kind === "ARRAY"
      )
        throw new Error(
          `Relationship "${name}" cannot use a nested reference array column; "${relation.column}" must be a UUID or UUID[] column.`,
        );
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
  return new DefinedTable<TColumns, TRelations, undefined>(columns, relations);
}
