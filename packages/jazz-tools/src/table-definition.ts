import { equalityIndexColumn, type EncryptionDeclaration } from "./e2ee/encrypted-schema.js";
import {
  assertRelationshipDeclaration,
  type Relationships,
  type ForwardRelationship,
  type Relationship,
} from "./relationships.js";
import {
  isComputedColumn,
  type AnyColumnDefinition,
  type ComputedColumn,
  type ColumnBuilderInitValue,
  type ColumnBuilderOptional,
  type ColumnBuilderSqlType,
  type ColumnBuilderValue,
} from "./dsl.js";
import {
  assertUserTableColumnNameAllowed,
  isProvenanceAccountColumn,
  type NoExplicitIdColumn,
  type ProvenanceAccountColumn,
} from "./magic-columns.js";
import { validateTransformDependencies } from "./runtime/column-transforms.js";
import type { TSTypeFromSqlType } from "./schema.js";

export type ReturnedColumnValue<TBuilder extends AnyColumnDefinition> =
  ColumnBuilderOptional<TBuilder> extends true
    ? ColumnBuilderValue<TBuilder> | null
    : ColumnBuilderValue<TBuilder>;

export type TableDefinition = Record<string, AnyColumnDefinition> & NoExplicitIdColumn;
type StoredColumnKeys<C extends TableDefinition> = {
  [K in keyof C & string]: C[K] extends ComputedColumn ? never : K;
}[keyof C & string];

// Wrap table columns so we can hang chained modifiers like .indexOnly(...) off tables
// without changing the column-level schema representation the runtime uses today.
export class DefinedTable<
  TColumns extends TableDefinition = TableDefinition,
  TRelations extends Relationships = Relationships,
  TEncryption extends EncryptionDeclaration | undefined = EncryptionDeclaration | undefined,
> {
  public readonly __jazzTableDefinition = true as const;

  encrypted<const Space extends StoredColumnKeys<TColumns>>(options: {
    space: Space;
    columns?: readonly StoredColumnKeys<TColumns>[];
    indexes?: Partial<Record<StoredColumnKeys<TColumns>, "equality">>;
  }): DefinedTable<TColumns, TRelations, EncryptionDeclaration & { space: Space }> {
    const referenceColumns = new Set<string>();
    for (const relation of Object.values(this.relations)) {
      if (relation.kind === "forward") referenceColumns.add(relation.column);
    }
    const selected: readonly string[] =
      options.columns ??
      Object.keys(this.columns).filter(
        (name) => !referenceColumns.has(name) && !isComputedColumn(this.columns[name]!),
      );
    for (const name of selected) {
      if (!(name in this.columns)) throw new Error(`Unknown encrypted column "${name}"`);
      if (referenceColumns.has(name)) {
        throw new Error(`Encrypted column "${name}" must not be a reference`);
      }
      const builder = this.columns[name]!;
      if (isComputedColumn(builder))
        throw new Error(`Computed field "${name}" cannot be encrypted`);
      const column = builder._build(name);
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
    const spaceBuilder = this.columns[options.space];
    const space =
      spaceBuilder && !isComputedColumn(spaceBuilder)
        ? spaceBuilder._build(options.space)
        : undefined;
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
    public readonly indexedColumns?: readonly StoredColumnKeys<TColumns>[],
    public readonly branchColumns?: readonly StoredColumnKeys<TColumns>[],
    public readonly compositeIndexes?: readonly (readonly StoredColumnKeys<TColumns>[])[],
    public readonly encryption?: TEncryption,
  ) {
    validateTransformDependencies(columns);
    for (const column of Object.keys(columns)) assertUserTableColumnNameAllowed(column);
  }

  indexOnly<
    const TColumnsForIndex extends readonly [
      StoredColumnKeys<TColumns>,
      ...StoredColumnKeys<TColumns>[],
    ],
  >(columns: TColumnsForIndex): DefinedTable<TColumns, TRelations, TEncryption> {
    const normalizedColumns = [...columns] as StoredColumnKeys<TColumns>[];
    for (const column of normalizedColumns) {
      if (!(column in this.columns) || isComputedColumn(this.columns[column]!)) {
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
      StoredColumnKeys<TColumns>,
      StoredColumnKeys<TColumns>,
      ...StoredColumnKeys<TColumns>[],
    ],
  >(columns: TColumnsForIndex): DefinedTable<TColumns, TRelations, TEncryption> {
    if (
      new Set(columns).size !== columns.length ||
      columns.some((column) => !(column in this.columns) || isComputedColumn(this.columns[column]!))
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

  branchBy<const TBranchColumn extends StoredColumnKeys<TColumns>>(
    column: TBranchColumn,
  ): DefinedTable<TColumns, TRelations, TEncryption>;
  branchBy<
    const TBranchColumns extends readonly [
      StoredColumnKeys<TColumns>,
      ...StoredColumnKeys<TColumns>[],
    ],
  >(columns: TBranchColumns): DefinedTable<TColumns, TRelations, TEncryption>;
  branchBy(
    columns:
      | StoredColumnKeys<TColumns>
      | readonly [StoredColumnKeys<TColumns>, ...StoredColumnKeys<TColumns>[]],
  ): DefinedTable<TColumns, TRelations, TEncryption> {
    const normalizedColumns = (
      Array.isArray(columns) ? [...columns] : [columns]
    ) as StoredColumnKeys<TColumns>[];
    for (const column of normalizedColumns) {
      if (!(column in this.columns) || isComputedColumn(this.columns[column]!)) {
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
  [K in StoredColumnKeys<T>]: ColumnBuilderSqlType<T[K]> extends
    | "UUID"
    | { kind: "ARRAY"; element: "UUID" }
    ? K
    : never;
}[StoredColumnKeys<T>];
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
export function defineTable<
  const TColumns extends TableDefinition,
  const TFields extends Record<
    string,
    Relationship | ComputedField<ComputedRow<TColumns>, unknown>
  >,
>(
  columns: TColumns & ValidateTransformContexts<TColumns>,
  fields: TFields,
  ..._validation: [TFields] extends [
    ValidateLocalRelations<TColumns, TableRelations<TFields>> & {
      [K in keyof TFields]: K extends keyof TColumns | "id" ? never : TFields[K];
    },
  ]
    ? []
    : [invalidFields: never]
): DefinedTable<TColumns & ComputedColumns<TFields>, TableRelations<TFields>, undefined> {
  if (!fields || typeof fields !== "object" || Array.isArray(fields))
    throw new Error(
      "s.table(columns, fields) requires a relationship map, optionally containing computed fields; use {} for no fields.",
    );
  let completeColumns: Record<string, AnyColumnDefinition> = columns;
  const relations: Relationships = {};
  const targets = new Map<string, string>();
  for (const [name, relation] of Object.entries(fields)) {
    const computed = relation && typeof relation === "object" && "_computed" in relation;
    if (
      !name ||
      ["__proto__", "constructor", "prototype"].includes(name) ||
      name.startsWith("$") ||
      name === "id" ||
      Object.hasOwn(columns, name)
    )
      throw new Error(
        `${computed ? "Computed field" : "Relationship"} "${name}" collides with a column.`,
      );
    if (computed) {
      assertUserTableColumnNameAllowed(name);
      if (completeColumns === columns) completeColumns = { ...columns };
      completeColumns[name] = {
        _computed: {
          ...relation._computed,
          rowColumns: Object.freeze(["id", ...Object.keys(columns)]),
        },
      } as ComputedColumn;
      continue;
    }
    assertRelationshipDeclaration(relation, name);
    relations[name] = relation;
    if (relation.kind === "forward") {
      const previous = targets.get(relation.column);
      if (previous && previous !== relation.table)
        throw new Error(`Conflicting relationship targets for column "${relation.column}".`);
      targets.set(relation.column, relation.table);
      if (isProvenanceAccountColumn(relation.column)) continue;
      const builder = columns[relation.column];
      const column =
        Object.hasOwn(columns, relation.column) && builder && !isComputedColumn(builder)
          ? builder._build(relation.column)
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
  return new DefinedTable(
    completeColumns as TColumns & ComputedColumns<TFields>,
    relations as TableRelations<TFields>,
  );
}

type ComputedRow<C extends TableDefinition> = { readonly id: string } & {
  readonly [K in keyof C & string]: ReturnedColumnValue<C[K]>;
};
export type ComputedField<R, V> = {
  readonly _computed: {
    readonly compute: (row: Readonly<R>) => V;
  };
};
type ComputedColumns<F> = [F] extends [never]
  ? {}
  : {
      [K in keyof F as F[K] extends ComputedField<never, unknown>
        ? K
        : never]: F[K] extends ComputedField<never, infer V> ? ComputedColumn<V> : never;
    };
type TableRelations<F> = [F] extends [never]
  ? never
  : {
      [K in keyof F as F[K] extends Relationship ? K : never]: Extract<F[K], Relationship>;
    };

/** Declare a virtual, read-only field in s.table's second argument. */
export function defineComputed<R, V>(compute: (row: Readonly<R>) => V): ComputedField<R, V> {
  if (typeof compute !== "function")
    throw new TypeError("s.computed(...) requires a synchronous callback.");
  return Object.freeze({ _computed: Object.freeze({ compute }) });
}

type StoredDependencyValue<C extends AnyColumnDefinition> =
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
      : Exclude<D, StoredColumnKeys<C> | "id"> extends never
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
